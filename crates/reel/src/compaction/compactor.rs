//! The maintenance-plane driver: dead-space compaction and the crc scrub
//!
//! A sealed segment past the rewrite threshold has its live records copied into an
//! active tail and its file retired once those copies are durable, under an index
//! repoint guarded by sequence number. Both tasks are bounded per pass and priced
//! against a rate gate, and the bytes charged are read plus write.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::config::{ReelConfig, RepairPath, VolumeClass};
use crate::error::Result;
use crate::format::band::Band;
use crate::format::column::RecordKey;
use crate::format::footer::FooterEntry;
use crate::format::footer::{FooterPartition, SegmentFooter, FIXED_TAIL_LEN, NO_RECORD};
use crate::format::loc::{Loc, SegmentId};
use crate::format::lsn::Lsn;
use crate::format::record::{peek_key_width, read_u32_le, RecordHeader, HEADER_LEN};
use crate::format::segment_header::SegmentHeader;
use crate::index::map::ReelIndex;
use crate::reel::segment::{SegmentHandle, SegmentReader, READ_CHUNK};
use crate::reel::{Reel, ReelShared, NOTHING_PURGED};
use crate::sync::{lock, try_lock};

use crate::compaction::pressure::{GcPressure, PassPace, RateGate, RateLimiter};

/// Bytes at the end of a sealed segment holding its footer length and magic
const TRAILER_LEN: u64 = 8;

/// Earnings one scrub pass will carry over from a stretch with no ticks in it
///
/// A pass takes what the rate earned since the last one, and this bounds how much of a
/// quiet stretch it will honour at once.
const SCRUB_PASS_MAX: Duration = Duration::from_secs(60);

/// Payload bytes one rewrite stripe may hold between its fetch and its apply
const STRIPE_STAGE_BYTES: u64 = 256 * 1024 * 1024;

/// Ranges one fetch wave holds in flight at once
///
/// A wave goes down the driver's batch door, so a ring holds all of it outstanding.
const FETCH_DEPTH: usize = 8;

/// What rewriting one of a segment's records produced, whichever order reached it
enum Rewrote {
    /// A live record was copied, carrying this many footprint bytes
    Copied(u64),

    /// A live value went into a row, and no record was written for it
    Listed,

    /// The record was dead, corrupt, or below the purge floor
    Skipped,

    /// A tombstone was carried into the destination
    Carried,

    /// A tombstone was safe to drop
    Dropped,

    /// A corrupt record on a sole copy, left standing to pin its segment
    Rotted,
}

/// What handling one live-record copy produced
enum CopyStep {
    /// The record was copied, carrying this many footprint bytes
    Copied(u64),

    /// The value went into a row and no record was written for it
    Listed,

    /// The record was dead or corrupt, so nothing was copied
    Skipped,

    /// The record is corrupt and this volume is its only copy, so it stays
    Rotted,
}

/// What handling one tombstone produced
enum TombstoneStep {
    /// The tombstone was carried into the destination
    Carried,

    /// The tombstone was safe to drop
    Dropped,
}

/// One record read back from a segment during compaction, a merge or a scrub
pub struct SourceRecord {
    /// What the record says about itself, key and sequence number included
    pub header: RecordHeader,

    /// Where the record header begins within its segment
    pub offset: u32,
}

impl SourceRecord {
    /// Where this record sits, for the liveness check against the index
    pub fn loc(&self, segment: SegmentId) -> Loc {
        Loc::new(segment, self.offset, self.header.length)
    }

    /// Bytes the record occupies, framing included
    pub fn span(&self) -> u64 {
        self.header.span()
    }

    /// Offset the record's payload begins at within its segment
    pub fn payload_at(&self) -> u64 {
        u64::from(self.offset) + self.header.prefix_len()
    }
}

/// A point-in-time read of the maintenance counters
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CompactionCounters {
    /// Payload-plus-header bytes rewritten by compaction
    pub compaction_bytes: u64,

    /// Segments retired by copying their live records elsewhere
    pub segments_rewritten: u64,

    /// Segments retired with no live records, unlinked whole
    pub segments_unlinked_whole: u64,

    /// Tombstones carried into a destination segment
    pub tombstones_carried: u64,

    /// Tombstones dropped because nothing could resurrect their key
    pub tombstones_dropped: u64,

    /// Records the scrub or compaction found failing their checksum
    pub scrub_hits: u64,

    /// Records dropped for sitting below the purge floor rather than copied
    pub records_purged: u64,

    /// Values a rewrite put in a row and wrote no record for, costing no copied bytes
    pub rows_listed: u64,

    /// Sorted runs merge passes read together, the only trace a tick-driven merge leaves
    pub runs_merged: u64,

    /// Segments standing because a pass met rot, a gauge that falls when they retire
    pub segments_pinned_by_rot: u64,

    /// Bytes rewrite passes read back, counted at the reader's refills
    pub read_bytes: u64,
}

impl CompactionCounters {
    /// Fraction of retired segments that had to be rewritten rather than unlinked
    ///
    /// The closer to zero, the more reclamation was a plain unlink of a dead segment.
    pub fn move_ratio(&self) -> f64 {
        let retired = self.segments_rewritten + self.segments_unlinked_whole;
        if retired == 0 {
            0.0
        } else {
            self.segments_rewritten as f64 / retired as f64
        }
    }
}

struct Metrics {
    compaction_bytes: AtomicU64,
    segments_rewritten: AtomicU64,
    segments_unlinked_whole: AtomicU64,
    tombstones_carried: AtomicU64,
    tombstones_dropped: AtomicU64,
    scrub_hits: AtomicU64,
    records_purged: AtomicU64,
    rows_listed: AtomicU64,
    runs_merged: AtomicU64,
    read_bytes: AtomicU64,
}

impl Metrics {
    fn new() -> Metrics {
        Metrics {
            compaction_bytes: AtomicU64::new(0),
            segments_rewritten: AtomicU64::new(0),
            segments_unlinked_whole: AtomicU64::new(0),
            tombstones_carried: AtomicU64::new(0),
            tombstones_dropped: AtomicU64::new(0),
            scrub_hits: AtomicU64::new(0),
            records_purged: AtomicU64::new(0),
            rows_listed: AtomicU64::new(0),
            runs_merged: AtomicU64::new(0),
            read_bytes: AtomicU64::new(0),
        }
    }

    fn record_pass(&self, tally: &PassTally, read_bytes: u64) {
        if tally.had_live {
            self.segments_rewritten.fetch_add(1, Ordering::AcqRel);
            self.compaction_bytes
                .fetch_add(tally.copied_bytes, Ordering::AcqRel);
        } else {
            self.segments_unlinked_whole.fetch_add(1, Ordering::AcqRel);
        }
        self.tombstones_carried
            .fetch_add(tally.carried, Ordering::AcqRel);
        self.tombstones_dropped
            .fetch_add(tally.dropped, Ordering::AcqRel);
        self.rows_listed.fetch_add(tally.listed, Ordering::AcqRel);
        self.read_bytes.fetch_add(read_bytes, Ordering::AcqRel);
    }

    fn record_hits(&self, hits: u64) {
        self.scrub_hits.fetch_add(hits, Ordering::AcqRel);
    }

    fn record_purged(&self, purged: u64) {
        self.records_purged.fetch_add(purged, Ordering::AcqRel);
    }

    fn record_merge(&self, runs: u64) {
        self.runs_merged.fetch_add(runs, Ordering::AcqRel);
    }

    fn snapshot(&self) -> CompactionCounters {
        CompactionCounters {
            compaction_bytes: self.compaction_bytes.load(Ordering::Acquire),
            segments_rewritten: self.segments_rewritten.load(Ordering::Acquire),
            segments_unlinked_whole: self.segments_unlinked_whole.load(Ordering::Acquire),
            tombstones_carried: self.tombstones_carried.load(Ordering::Acquire),
            tombstones_dropped: self.tombstones_dropped.load(Ordering::Acquire),
            scrub_hits: self.scrub_hits.load(Ordering::Acquire),
            records_purged: self.records_purged.load(Ordering::Acquire),
            rows_listed: self.rows_listed.load(Ordering::Acquire),
            runs_merged: self.runs_merged.load(Ordering::Acquire),
            read_bytes: self.read_bytes.load(Ordering::Acquire),
            // the compactor holds the pins, so the gauge is filled by its reader
            segments_pinned_by_rot: 0,
        }
    }
}

/// What the fetch phase learned about a record, carried to the apply phase
///
/// Everything the fetch already asked the index is an answer the apply would ask for a
/// second time, which on a paged column is a footer search.
enum Staged {
    /// Nothing was staged, so the apply reads the payload and asks for itself
    Unread,

    /// The index no longer pointed at this record when the fetch looked
    Dead,

    /// Live when the fetch looked, with the bytes if the fetch read them
    Live(Option<Vec<u8>>),
}

/// What one pass accumulated while it rewrote
#[derive(Default)]
struct PassTally {
    copied_bytes: u64,
    listed: u64,
    carried: u64,
    dropped: u64,
    had_live: bool,
    rotted: bool,
}

/// Where the last scrub pass stopped, so the next one carries on from there
///
/// Nothing about it survives the process, which is why a fresh sweep starts at a
/// rotated segment rather than the lowest one.
#[derive(Clone, Copy, Default, Eq, PartialEq)]
struct ScrubCursor {
    /// Segment this sweep began at, which is also where its lap ends
    origin: SegmentId,

    /// Segment the next pass resumes in
    segment: SegmentId,

    /// Offset within that segment the next pass resumes at
    offset: u64,

    /// End of that segment's record region, so a resume need not read the footer again
    region_end: u64,

    /// Dead bytes counted in this segment so far, across however many passes
    dead: u64,
}

/// What one segment's share of a scrub pass produced
struct ScrubStep {
    /// Keys evicted for failing their checksum
    hits: usize,

    /// Dead bytes counted in this segment, including whatever was carried in
    dead: u64,

    /// Resume offset and record-region end, when the budget ran out mid-segment
    resume_at: Option<(u64, u64)>,
}

/// What a sealed segment's footer settles for good
///
/// Neither answer can change: the footer is down before either is asked, and a segment
/// is retired rather than rewritten in place. A tick asks both of every standing segment,
/// so they are derived once per segment rather than read out of the file per walk.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FooterFacts {
    /// Every partition's rows name ascending offsets, so the segment reads as one run
    pub is_sorted_run: bool,

    /// The rows in key order name offsets that do not ascend, so a rewrite has work
    pub is_out_of_order: bool,
}

/// The maintenance plane for one volume, shared by every reel it holds
pub struct Compactor {
    /// Free-space pressure and tiering for the volume
    pressure: GcPressure,

    /// Byte cadence compaction passes are charged against
    compact_rate: RateGate,

    /// Byte cadence scrub passes are charged against, nothing when the scrub is off
    scrub_rate: Option<RateGate>,

    /// Where the last scrub pass stopped, nothing at the start of a sweep
    scrub_cursor: Mutex<Option<ScrubCursor>>,

    /// Held for the length of one scrub pass, so passes never stack
    scrub_guard: Mutex<()>,

    /// Where this process starts a fresh sweep, so restarts do not all start alike
    scrub_seed: u64,

    /// Bytes of later ingest past which a fast segment's survivors demote
    demote_after_bytes: Option<u64>,

    /// Counters the maintenance plane publishes
    metrics: Metrics,

    /// Segments a pass is rewriting right now, so a second pass picks another
    in_flight: Mutex<std::collections::HashSet<SegmentId>>,

    /// Segments a pass left standing for rot, against the dead bytes it left them at
    rotted: Mutex<std::collections::HashMap<SegmentId, u64>>,

    /// What each sealed segment's footer said, so a walk over the standing ones reads
    /// none of them again
    sealed_facts: Mutex<std::collections::HashMap<SegmentId, FooterFacts>>,
}

/// Takes a segment out of the in-flight set however its pass leaves
pub struct PassClaim<'compactor> {
    compactor: &'compactor Compactor,
    segment: SegmentId,
}

impl PassClaim<'_> {
    /// The segment this claim is holding
    pub fn segment(&self) -> SegmentId {
        self.segment
    }
}

impl Drop for PassClaim<'_> {
    fn drop(&mut self) {
        lock(&self.compactor.in_flight).remove(&self.segment);
    }
}

impl Compactor {
    /// Claim one segment for a pass, or nothing when another pass already holds it
    ///
    /// The claim is what stops two passes reading and retiring the same file. A merge
    /// takes one per source and holds them all for the length of its pass.
    pub fn claim(&self, segment: SegmentId) -> Option<PassClaim<'_>> {
        lock(&self.in_flight).insert(segment).then_some(PassClaim {
            compactor: self,
            segment,
        })
    }

    /// Whether a pass left this segment standing because something in it rotted
    ///
    /// Membership alone rather than the dead-byte refinement the ranking uses: a merge
    /// reads every row of a source, so a segment holding a record that will not verify
    /// has nothing to offer one until compaction has been through it.
    pub fn is_rot_pinned(&self, segment: SegmentId) -> bool {
        lock(&self.rotted).contains_key(&segment)
    }

    /// Leave a segment standing for rot, at the dead bytes the pass left it with
    pub fn pin_rot(&self, segment: SegmentId, dead: u64) {
        lock(&self.rotted).insert(segment, dead);
    }

    /// What one sealed segment's footer says, read out of the file only on the first ask
    ///
    /// Nothing where the segment has no footer to read, and nothing held either: a seal
    /// the device refused gets another, and an answer kept here would outlive the refusal.
    pub fn facts_of(
        &self,
        shared: &Arc<ReelShared>,
        segment: SegmentId,
    ) -> Result<Option<FooterFacts>> {
        if let Some(facts) = lock(&self.sealed_facts).get(&segment).copied() {
            return Ok(Some(facts));
        }
        // read off the lock, or every miss holds the map across a device read
        let Some(footer) = shared.footer_of(segment)? else {
            return Ok(None);
        };
        let facts = footer_facts(&footer);
        lock(&self.sealed_facts).insert(segment, facts);
        Ok(Some(facts))
    }

    /// Drop what a retired segment's footer settled, since the file is going with it
    pub fn forget_facts(&self, segment: SegmentId) {
        lock(&self.sealed_facts).remove(&segment);
    }

    /// Book the runs one merge pass read together
    ///
    /// A pass the tick drove hands its report to nobody, so this is where a volume says
    /// whether its runs are being collapsed at all.
    pub fn note_merged_runs(&self, runs: u64) {
        self.metrics.record_merge(runs);
    }

    /// A meter for one pass of another kind, on the gate compaction is paced by
    ///
    /// The merge draws on the governor compaction draws on, since both are the same
    /// device under the same foreground and an operator sets one budget.
    pub fn pace(&self) -> PassPace<'_> {
        self.compact_rate.pace()
    }

    /// Whether a foreground write of this size fits outside the reserve
    ///
    /// The reserve is what compaction works in, so a foreground write may not spend it.
    pub fn can_admit_foreground(&self, used_bytes: u64, request_bytes: u64) -> bool {
        self.pressure
            .can_admit_foreground(used_bytes, request_bytes)
    }

    /// A maintenance plane sized from the volume settings and the disk under it
    ///
    /// A capacity of zero leaves the pressure model unbounded, so the append-only
    /// deadlock guard is off: a full disk cannot be compacted out of, because compaction
    /// needs somewhere to write the survivors.
    pub fn new(config: &ReelConfig, capacity_bytes: u64, fast_capacity_bytes: u64) -> Compactor {
        // One segment for compaction to write survivors into, plus one per tail, since
        // between two maintenance ticks every tail can roll and claim a fresh segment.
        let reserve = config.segment_bytes.to_bytes() * (config.tail_count() as u64 + 1);
        // Half the fast tier of later ingest: late enough that the hot set stays hot,
        // early enough that the tier never fills before demotion starts.
        let demote_after_bytes = (fast_capacity_bytes > 1).then_some(fast_capacity_bytes / 2);
        let compact = RateLimiter::for_compaction(config.compact_mbps);
        let scrub = RateLimiter::for_scrub(config.scrub_mbps, compact.target_mbps());
        Compactor {
            pressure: GcPressure::new(capacity_bytes, reserve, config.compact_dead_ratio),
            compact_rate: RateGate::new(compact),
            scrub_rate: scrub.map(RateGate::new),
            scrub_cursor: Mutex::new(None),
            scrub_guard: Mutex::new(()),
            in_flight: Mutex::new(std::collections::HashSet::new()),
            rotted: Mutex::new(std::collections::HashMap::new()),
            sealed_facts: Mutex::new(std::collections::HashMap::new()),
            // nothing about the sweep survives the process, so the rotation comes from
            // something that differs between runs of it
            scrub_seed: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|since| since.as_nanos() as u64)
                .unwrap_or(0),
            demote_after_bytes,
            metrics: Metrics::new(),
        }
    }

    /// Free-space pressure and tiering for the volume
    pub fn pressure(&self) -> &GcPressure {
        &self.pressure
    }

    /// The resolved compaction rate cap in megabytes per second
    pub fn compaction_rate_mbps(&self) -> u64 {
        self.compact_rate.target_mbps()
    }

    /// Whether the compaction rate allows another pass to start now
    ///
    /// A paced pass pays for its steps as it takes them, so what stands here is the
    /// tail past its last step rather than the whole of what it moved.
    pub fn is_compaction_due(&self) -> bool {
        self.compact_rate.is_open()
    }

    /// The resolved scrub rate, or nothing when the scrub is disabled
    pub fn scrub_rate_mbps(&self) -> Option<u64> {
        self.scrub_rate.as_ref().map(RateGate::target_mbps)
    }

    /// Whether the scrub task runs at all
    pub fn is_scrub_enabled(&self) -> bool {
        self.scrub_rate.is_some()
    }

    /// Where the next scrub pass picks up, or nothing when the sweep is at its start
    pub fn scrub_resume_point(&self) -> Option<(SegmentId, u64)> {
        lock(&self.scrub_cursor).map(|cursor| (cursor.segment, cursor.offset))
    }

    /// A read of the maintenance counters
    pub fn counters(&self) -> CompactionCounters {
        CompactionCounters {
            segments_pinned_by_rot: lock(&self.rotted).len() as u64,
            ..self.metrics.snapshot()
        }
    }

    /// The sealed segment with the highest dead fraction past the threshold
    ///
    /// A clustered volume with nothing to reclaim falls back to a rewrite for order.
    pub fn select_target(
        &self,
        reel: &Reel,
        index: &ReelIndex,
        effective_dead_ratio: f64,
        cue_floor: Option<Lsn>,
    ) -> Option<(SegmentId, f64)> {
        if index.has_pending_covers() {
            return None;
        }
        if let Some(ranked) = self.select_ranked(reel, index, effective_dead_ratio, cue_floor) {
            return Some(ranked);
        }
        let fallback = self.select_unsorted(reel, index, cue_floor);
        if let Some((segment, _)) = fallback {
            lock(&self.in_flight).insert(segment);
        }
        fallback
    }

    /// A sealed segment with nothing live left in it, which retires by unlink
    ///
    /// The ranking alone: a clustering rewrite copies every live record forward, which
    /// is the opposite of an unlink.
    pub fn select_whole_dead(
        &self,
        reel: &Reel,
        index: &ReelIndex,
        cue_floor: Option<Lsn>,
    ) -> Option<(SegmentId, f64)> {
        if index.has_pending_covers() {
            return None;
        }
        self.select_ranked(reel, index, 1.0, cue_floor)
    }

    /// The ranking walk itself, claiming whatever it chooses
    ///
    /// Nothing retires while a cover is still owed its sweep, which both callers check
    /// before they get here: an unsettled record would leave the shard counters holding
    /// a segment that is gone.
    fn select_ranked(
        &self,
        reel: &Reel,
        index: &ReelIndex,
        effective_dead_ratio: f64,
        cue_floor: Option<Lsn>,
    ) -> Option<(SegmentId, f64)> {
        let shared = reel.shared();
        let mut best: Option<(SegmentId, f64)> = None;
        // one pass rather than two, since this lock is the one every insert takes
        let (segments, floors) = index.ranking();
        // read before the claim, so the two locks are never held at once
        let pinned = lock(&self.rotted).clone();
        // A segment whose spans the index has not been told about yet answers every
        // liveness question with nothing, so a pass over one would read its records as
        // dead and retire the file they were in.
        let owed = shared.pending_seals();
        // the list is a copy, so everything sealed from here on is invisible to the walk
        crate::sync::rendezvous::at("compaction/owed");
        let claimed = lock(&self.in_flight);
        for (segment, bytes) in segments {
            if shared.is_held(segment) || claimed.contains(&segment) || owed.contains(&segment) {
                continue;
            }
            let total = bytes.total();
            if total == 0 {
                continue;
            }
            // A cue point can still read versions this segment holds: dead only means
            // no live entry points at them, which is what an older reader came for.
            if cue_floor.is_some_and(|floor| {
                index
                    .min_lsn_of(segment)
                    .is_some_and(|oldest| oldest <= floor)
            }) {
                continue;
            }
            // dead bytes plus whatever tombstones this segment could stop carrying
            let fraction = bytes.reclaimable(floors.excluding(segment)) as f64 / total as f64;
            if fraction < effective_dead_ratio {
                continue;
            }
            // A segment pinned by rot is worth another pass only once something in it
            // has died since: otherwise a pass reads the whole file, meets the same
            // checksum miss and leaves it standing again.
            if pinned.get(&segment) == Some(&bytes.dead) {
                continue;
            }
            let is_better = best.map(|existing| fraction > existing.1).unwrap_or(true);
            if is_better {
                best = Some((segment, fraction));
            }
        }
        // claimed under the lock the choice was made under, or two callers arriving
        // together both leave with the same segment
        if let Some((segment, _)) = best {
            // A segment that sealed inside the walk reads as a tail just freed: in the
            // ranking, absent from a queue taken before its note existed. Asking the
            // queue again settles it, since a seal queues its note before it gives up
            // its hold.
            if shared.pending_seals().contains(&segment) {
                return None;
            }
            let mut claimed = claimed;
            claimed.insert(segment);
        }
        best
    }

    /// A sealed segment whose records are not in key order, for a clustered volume
    ///
    /// The other reason to rewrite: not to reclaim space but to put records in the order
    /// their keys are in, which is what makes a segment a sorted run. A segment is
    /// already one exactly when the footer's rows in key order have ascending offsets.
    fn select_unsorted(
        &self,
        reel: &Reel,
        index: &ReelIndex,
        cue_floor: Option<Lsn>,
    ) -> Option<(SegmentId, f64)> {
        if !reel.shared().config.rewrite_on_seal {
            return None;
        }
        let shared = reel.shared();
        let (segments, _) = index.ranking();
        let pinned = lock(&self.rotted).clone();
        let owed = shared.pending_seals();
        for (segment, bytes) in segments {
            if shared.is_held(segment) || bytes.total() == 0 || owed.contains(&segment) {
                continue;
            }
            // rewriting for order meets the same rot as rewriting for space
            if pinned.get(&segment) == Some(&bytes.dead) {
                continue;
            }
            if cue_floor.is_some_and(|floor| {
                index
                    .min_lsn_of(segment)
                    .is_some_and(|oldest| oldest <= floor)
            }) {
                continue;
            }
            // which order a sealed footer's offsets are in was settled when it went down
            let Some(facts) = self.facts_of(shared, segment).ok().flatten() else {
                continue;
            };
            if !facts.is_out_of_order {
                continue;
            }
            // the same stale copy the ranked walk guards against, for the same reason
            if shared.pending_seals().contains(&segment) {
                continue;
            }
            return Some((segment, 0.0));
        }
        None
    }

    /// Rewrite one sealed segment's live records and retire it
    ///
    /// A segment whose file is already gone leaves nothing to reclaim, so what it left
    /// in the counters is dropped instead.
    pub fn compact_segment(
        &self,
        reel: &Reel,
        index: &ReelIndex,
        segment: SegmentId,
    ) -> Result<()> {
        // however this pass leaves, the claim drops: a leaked one is a segment nothing
        // ever compacts again
        let _claim = PassClaim {
            compactor: self,
            segment,
        };
        let shared = reel.shared();
        if reel.tails().is_empty() {
            return Ok(());
        }
        // What the other segments hold is only half the floor: a number drawn before
        // this pass can still be published into a segment after it, under an lsn no
        // floor has seen, so a tombstone above what is settled has to come across.
        let settled = shared.settled_below();
        let drop_floor = index
            .min_lsn_excluding(segment)
            .map_or(settled, |floor| floor.min(settled));
        let source = match source_handle(shared, segment) {
            Ok(source) => source,
            Err(error) if is_missing(&error) => {
                index.forget_segment(segment);
                lock(&self.rotted).remove(&segment);
                self.forget_facts(segment);
                return Ok(());
            }
            Err(error) => return Err(error),
        };
        let file_len = match segment_len(shared, &source)? {
            Some(len) => len,
            // the handle is fresh, so a missing length is a missing file rather than a
            // stale descriptor
            None => {
                index.forget_segment(segment);
                lock(&self.rotted).remove(&segment);
                self.forget_facts(segment);
                return Ok(());
            }
        };
        let region_end = footer_bound(shared, &source, file_len)?;
        // key order where the segment can say what that is, offset order otherwise
        let order = self.rewrite_order(shared, segment);
        let mut reader = SegmentReader::new(&shared.driver, source.file(), region_end);
        // The band the source was drawn under is where its survivors belong. Placement
        // the writer paid for is undone otherwise: every rewrite would put a window's
        // records back into the mixture they were kept out of.
        let band = band_of(shared, &source)?;
        let dest_index = destination(reel, band)?;

        // Only the reserved tail answers to a named tier and to the source's band, since
        // a foreground destination mixes fresh puts in and fresh puts stay fast. A
        // leftover active segment from another tier or another band is sealed away so
        // the swap draws under what this pass just set.
        if reel.reserved_tail() == Some(dest_index) {
            let class = self.output_class(shared, segment);
            let dest = &reel.tails()[dest_index];
            dest.set_draw_class(class);
            dest.set_band(band)?;
            let active = dest.tail().active_segment();
            if shared.volumes.class_of(shared.volumes.root_of(active)) != class {
                dest.seal()?;
            }
        }

        // created where the charged reads begin, so the footer the order came from is
        // off the gate's books
        let mut pace = self.compact_rate.pace();

        let mut tally = PassTally::default();
        let copy_result = match &order {
            Some((order, prefix_bound)) => self.rewrite_ordered(
                reel,
                dest_index,
                index,
                &mut reader,
                order,
                *prefix_bound,
                segment,
                drop_floor,
                &mut tally,
                &mut pace,
            ),
            None => self.rewrite_scanning(
                reel,
                dest_index,
                index,
                &mut reader,
                segment,
                drop_floor,
                &mut tally,
                &mut pace,
            ),
        };

        // the source's standing rows come across before the flush, since the scan above
        // only ever saw records and these have none
        match self.relist_standing(reel, index, dest_index, segment) {
            Ok((moved, rot)) => {
                tally.listed += moved;
                tally.had_live |= moved > 0;
                tally.rotted |= rot;
            }
            Err(error) => {
                drop(source);
                return Err(error);
            }
        }

        if let Err(error) = reel.tails()[dest_index].flush() {
            drop(source);
            return Err(error);
        }
        if let Err(error) = copy_result {
            drop(source);
            return Err(error);
        }

        // The gate prices the device, so the pass is charged what its reader bought,
        // refills at dead records included, plus the copies it wrote.
        let read_bytes = reader.read_bytes();
        let charged = read_bytes.saturating_add(tally.copied_bytes);

        // A rotted record on a sole copy keeps its segment: the key still resolves into
        // this file, and retiring it would unlink the last copy of those bytes.
        if tally.rotted {
            drop(source);
            // pinned at the dead bytes left behind, so the ranking does not offer the
            // segment back on the next tick
            lock(&self.rotted).insert(segment, index.segment_bytes(segment).dead);
            self.metrics.record_pass(&tally, read_bytes);
            pace.settle(charged);
            return Ok(());
        }

        // A cover that went up while this pass ran may span rows it skipped as covered,
        // and retiring the segment would take them out from under the release pass.
        if index.has_pending_covers() {
            drop(source);
            self.metrics.record_pass(&tally, read_bytes);
            pace.settle(charged);
            return Ok(());
        }

        // The destination is closed before the source is retired, since a listed row is
        // not on disk until the seal writes it, and the error is propagated because a
        // seal that fails must leave the source standing. Closing per pass also keeps
        // one source's records to one destination, so key and offset order agree in it.
        if tally.had_live {
            if let Some(reserved) = reel.reserved_tail() {
                let closed = reel.tails()[reserved].seal()?;
                // A listed row has no index entry, so noting the destination's spans
                // ahead of the retire closes a window where an acknowledged key exists
                // nowhere a read looks.
                if tally.listed > 0 {
                    if let Some(footer) = shared.footer_of(closed)? {
                        index.note_spans(closed, &footer)?;
                    }
                }
            }
        }

        // The index stops naming the segment before the file goes, not after: a paged
        // read chooses its segment from a footer search, and the other order offers one
        // whose file is already unlinked.
        crate::sync::rendezvous::at("compaction/retire");
        index.forget_segment(segment);
        lock(&self.rotted).remove(&segment);
        self.forget_facts(segment);
        shared.fd_cache.remove(segment);
        // the footer goes with the file, or it answers for a segment that is not there
        shared.footers.forget(segment);
        source.mark_doomed();
        drop(source);

        self.metrics.record_pass(&tally, read_bytes);
        pace.settle(charged);
        Ok(())
    }

    /// Rewrite a segment the footer gave an order for: fetch by offset, apply by key
    ///
    /// Only the applies need key order, since theirs is the order records land in the
    /// destination. The two orders meet in a stripe: fetch its records ascending down
    /// the driver's batch door, hold their payloads, then apply the stripe in key
    /// order, stripes themselves in key order so the destination stays one sorted run.
    ///
    /// A wave is outstanding as one unit, so the gate can interrupt the fetch between
    /// waves and nowhere inside one.
    #[allow(clippy::too_many_arguments)]
    fn rewrite_ordered(
        &self,
        reel: &Reel,
        dest_index: usize,
        index: &ReelIndex,
        reader: &mut SegmentReader<'_>,
        order: &[(u32, u32)],
        prefix_bound: u64,
        segment: SegmentId,
        drop_floor: Lsn,
        tally: &mut PassTally,
        pace: &mut PassPace<'_>,
    ) -> Result<()> {
        let mut pool: Vec<Vec<u8>> = Vec::new();
        let mut start = 0usize;
        while start < order.len() {
            let end = stripe_end(order, start);
            let mut plan: Vec<(usize, u32)> = (start..end)
                .map(|position| (position, order[position].0))
                .collect();
            plan.sort_unstable_by_key(|&(_, offset)| offset);
            let ranges = chunk_ranges(&plan, order, prefix_bound, reader.limit());

            let mut staged: Vec<(usize, SourceRecord, Staged)> = Vec::with_capacity(plan.len());
            let mut wave_at = 0usize;
            while wave_at < ranges.len() {
                let wave =
                    &ranges[wave_at..wave_at + wave_len(&ranges[wave_at..], pace.step_bytes())];
                wave_at += wave.len();
                let asks: Vec<(u64, usize)> =
                    wave.iter().map(|range| (range.start, range.len)).collect();
                let buffers = reader.read_ranges(&asks, &mut pool)?;
                for (range, buffer) in wave.iter().zip(buffers) {
                    pool.push(reader.preload(range.start, buffer));
                    for &(position, offset) in &plan[range.members.clone()] {
                        let record =
                            match RecordScan::resuming(reader, u64::from(offset)).next_record()? {
                                Some(record) => record,
                                // a footer offset the records disagree with gives up
                                // the record, not the pass
                                None => continue,
                            };
                        let payload = self.stage_payload(reel, index, reader, &record, segment)?;
                        staged.push((position, record, payload));
                    }
                }
                pace.reached(reader.read_bytes() + tally.copied_bytes);
            }
            staged.sort_unstable_by_key(|entry| entry.0);

            for (_, record, payload) in staged {
                self.apply_one(
                    reel, dest_index, index, reader, &record, payload, segment, drop_floor, tally,
                )?;
                pace.reached(reader.read_bytes() + tally.copied_bytes);
            }
            start = end;
        }
        Ok(())
    }

    /// Rewrite a segment with no footer by walking its records where they lie
    ///
    /// Metered per record, so the stretch the gate cannot interrupt is one window
    /// refill, whatever the rate asks for.
    #[allow(clippy::too_many_arguments)]
    fn rewrite_scanning(
        &self,
        reel: &Reel,
        dest_index: usize,
        index: &ReelIndex,
        reader: &mut SegmentReader<'_>,
        segment: SegmentId,
        drop_floor: Lsn,
        tally: &mut PassTally,
        pace: &mut PassPace<'_>,
    ) -> Result<()> {
        let mut at = 0u64;
        loop {
            let record = match RecordScan::resuming(reader, at).next_record()? {
                Some(record) => {
                    at = u64::from(record.offset) + record.header.span();
                    record
                }
                None => return Ok(()),
            };
            self.apply_one(
                reel,
                dest_index,
                index,
                reader,
                &record,
                Staged::Unread,
                segment,
                drop_floor,
                tally,
            )?;
            pace.reached(reader.read_bytes() + tally.copied_bytes);
        }
    }

    /// The payload a stripe fetch holds for the apply, where one will be wanted
    ///
    /// Nothing is staged for a record the apply is going to skip, since its bytes would
    /// fill the cap with dead weight.
    fn stage_payload(
        &self,
        reel: &Reel,
        index: &ReelIndex,
        reader: &mut SegmentReader<'_>,
        record: &SourceRecord,
        segment: SegmentId,
    ) -> Result<Staged> {
        let header = &record.header;
        if header.flags.is_range_tombstone() {
            return read_payload(reader, record).map(|end| Staged::Live(Some(end)));
        }
        if header.flags.is_tombstone() {
            // a point tombstone has no payload to stage and asks nothing of the index
            return Ok(Staged::Unread);
        }
        let live =
            matches!(index.get(&header.key)?, Some(entry) if entry.loc == record.loc(segment));
        if !live {
            return Ok(Staged::Dead);
        }
        if is_purged(reel.shared().purge_floor(), index, &header.key) {
            return Ok(Staged::Live(None));
        }
        read_payload(reader, record).map(|payload| Staged::Live(Some(payload)))
    }

    /// Rewrite one record and fold what happened into the pass's tally
    #[allow(clippy::too_many_arguments)]
    fn apply_one(
        &self,
        reel: &Reel,
        dest_index: usize,
        index: &ReelIndex,
        reader: &mut SegmentReader<'_>,
        record: &SourceRecord,
        staged: Staged,
        segment: SegmentId,
        drop_floor: Lsn,
        tally: &mut PassTally,
    ) -> Result<()> {
        match self.rewrite_one(
            reel, dest_index, index, reader, record, staged, segment, drop_floor,
        )? {
            Rewrote::Copied(span) => {
                tally.copied_bytes += span;
                tally.had_live = true;
            }
            // A listed row is live work like a copy, so the segment has something to
            // seal. Its span stays out of the copied bytes, which gauge write
            // amplification, since a listed value writes no record at all.
            Rewrote::Listed => {
                tally.had_live = true;
                tally.listed += 1;
            }
            Rewrote::Skipped => {}
            Rewrote::Carried => tally.carried += 1,
            Rewrote::Dropped => tally.dropped += 1,
            Rewrote::Rotted => tally.rotted = true,
        }
        Ok(())
    }

    /// Rewrite one of a segment's records, whichever order the caller reached it in
    #[allow(clippy::too_many_arguments)]
    fn rewrite_one(
        &self,
        reel: &Reel,
        dest_index: usize,
        index: &ReelIndex,
        reader: &mut SegmentReader<'_>,
        record: &SourceRecord,
        staged: Staged,
        segment: SegmentId,
        drop_floor: Lsn,
    ) -> Result<Rewrote> {
        if record.header.flags.is_tombstone() || record.header.flags.is_range_tombstone() {
            return Ok(
                match self
                    .carry_or_drop(reel, dest_index, index, reader, record, staged, drop_floor)?
                {
                    TombstoneStep::Carried => Rewrote::Carried,
                    TombstoneStep::Dropped => Rewrote::Dropped,
                },
            );
        }
        Ok(
            match self.copy_live(reel, dest_index, index, reader, record, staged, segment)? {
                CopyStep::Copied(span) => Rewrote::Copied(span),
                CopyStep::Listed => Rewrote::Listed,
                CopyStep::Skipped => Rewrote::Skipped,
                CopyStep::Rotted => Rewrote::Rotted,
            },
        )
    }

    /// A segment's records in key order, per column, as offset and length pairs
    ///
    /// A sealed segment's footer is already a sorted index of what it holds, so applying
    /// records in that order is what makes the destination sorted on disk. The offsets
    /// are trusted only as an order: every record is still read, framed and checksum
    /// verified where it lands.
    fn rewrite_order(
        &self,
        shared: &Arc<ReelShared>,
        segment: SegmentId,
    ) -> Option<(Vec<(u32, u32)>, u64)> {
        let footer = shared.footer_of(segment).ok().flatten()?;
        footer_order(&footer)
    }

    /// Punch the dead runs out of sealed segments, and say what came back
    ///
    /// Sealed, footer-bearing segments only: a rebuild reads those from their footers
    /// rather than by walking them, so a punched record's zeroed header never ends a
    /// recovery walk. Only records the index has already moved past are punched.
    pub fn erase_dead_runs(&self, reel: &Reel, index: &ReelIndex) -> Result<EraseReport> {
        let shared = reel.shared();
        let mut report = EraseReport::default();
        for (segment, _) in index.segments_snapshot() {
            if shared.is_held(segment) {
                continue;
            }
            let Some(footer) = shared.footer_of(segment).ok().flatten() else {
                continue;
            };
            // The footer's rows drive the walk, not a sequential scan: a hole an earlier
            // pass left reads as zeroed headers, which a scan takes for the end of data.
            let mut offsets: Vec<u32> = Vec::new();
            for partition in &footer.partitions {
                for at in 0..partition.len() {
                    let row = partition.row_at(at)?;
                    if row.stands_alone() || !row.flags.is_data() {
                        continue;
                    }
                    offsets.push(row.offset);
                }
            }
            offsets.sort_unstable();
            let source = match source_handle(shared, segment) {
                Ok(source) => source,
                Err(error) if is_missing(&error) => continue,
                Err(error) => return Err(error),
            };
            let file_len = match segment_len(shared, &source)? {
                Some(len) => len,
                None => continue,
            };
            let region_end = footer_bound(shared, &source, file_len)?;
            let mut reader = SegmentReader::new(&shared.driver, source.file(), region_end);
            let mut runs: Vec<(u64, u64)> = Vec::new();
            for offset in offsets {
                let record =
                    match RecordScan::resuming(&mut reader, u64::from(offset)).next_record()? {
                        // a row whose record no longer parses is one an earlier pass erased
                        None => continue,
                        Some(record) if record.offset != offset => continue,
                        Some(record) => record,
                    };
                let live = matches!(
                    index.get(&record.header.key)?,
                    Some(entry) if entry.loc == record.loc(segment)
                );
                if live {
                    continue;
                }
                let start = u64::from(record.offset);
                let end = start + record.span();
                match runs.last_mut() {
                    Some(run) if run.1 == start => run.1 = end,
                    _ => runs.push((start, end)),
                }
            }
            drop(source);

            report.segments += 1;
            let file = std::fs::OpenOptions::new()
                .write(true)
                .open(shared.segment_path(segment))?;
            for (start, end) in runs {
                report.dead_run_bytes += end - start;
                let hole_start = start.next_multiple_of(ERASE_BLOCK);
                let hole_end = (end / ERASE_BLOCK) * ERASE_BLOCK;
                if hole_end > hole_start {
                    erase_range(&file, hole_start, hole_end - hole_start)?;
                    report.erased_bytes += hole_end - hole_start;
                }
            }
        }
        Ok(report)
    }

    /// A row that can be this value's only home, when everything about it allows one
    ///
    /// Every condition here is a way to lose the value rather than a preference: the
    /// volume and the destination have to allow listing, the record has to be
    /// uncompressed, the key must be gone from the resident map, and the row itself has
    /// to be able to hold the value.
    fn carried_row(
        &self,
        reel: &Reel,
        index: &ReelIndex,
        dest_index: usize,
        record: &SourceRecord,
        payload: &[u8],
    ) -> Option<FooterEntry> {
        if !self.may_list(reel, dest_index) {
            return None;
        }
        if record.header.codec != 0 {
            return None;
        }
        // A key still in the resident map names this record from RAM and nothing
        // repoints it when the row is listed, so the record is copied instead and a
        // later pass lists it once the key has paged out.
        if index
            .column(record.header.key.column)
            .and_then(|column| column.entry_or_grave(record.header.key.as_slice()))
            .is_some()
        {
            return None;
        }
        let carry = index.spec(record.header.key.column)?.row_carry_width();
        FooterEntry::standing_alone(
            record.header.key.clone(),
            record.header.lsn,
            record.header.flags,
            carry,
            payload,
        )
    }

    /// Whether this pass may put a value in a row and write no record for it
    ///
    /// Both conditions are about who holds the only copy: the volume has to page, since
    /// a resident entry would keep naming a record about to be unlinked, and the
    /// destination has to be the tail this pass seals itself.
    fn may_list(&self, reel: &Reel, dest_index: usize) -> bool {
        reel.shared().config.index.pages() && reel.reserved_tail() == Some(dest_index)
    }

    /// Carry a source's standing rows into the destination, since no record holds them
    ///
    /// The scan walks records and a listed row has none, so without this a pass over a
    /// segment holding one would retire it and take the only copy of its value. A row
    /// the index no longer resolves to is dropped, which is how one is reclaimed.
    fn relist_standing(
        &self,
        reel: &Reel,
        index: &ReelIndex,
        dest_index: usize,
        segment: SegmentId,
    ) -> Result<(u64, bool)> {
        if !self.may_list(reel, dest_index) {
            return Ok((0, false));
        }
        let Some(footer) = reel.shared().footer_of(segment)? else {
            return Ok((0, false));
        };
        let mut listed = 0u64;
        let mut rotted = false;
        for partition in &footer.partitions {
            let Some(carry) = index
                .spec(partition.column)
                .map(|spec| spec.row_carry_width())
            else {
                continue;
            };
            for at in 0..partition.len() {
                let row = partition.row_at(at)?;
                if !row.stands_alone() {
                    continue;
                }
                let Some(bytes) = partition.key_at(at) else {
                    continue;
                };
                let key = RecordKey::from_bytes(partition.column, bytes)?;
                let key_of = key.clone();
                // the row has to still be the version the volume answers with
                let loc = Loc::new(segment, NO_RECORD, row.len);
                match index.get(&key)? {
                    Some(entry) if entry.loc == loc => {}
                    Some(_) | None => continue,
                }
                // a row that will not verify is the sole copy of its value, so its
                // segment is kept the way a rotted record's is
                let value = match partition.carried_at(at) {
                    Ok(Some(value)) => value,
                    Ok(None) | Err(_) => {
                        self.metrics.record_hits(1);
                        rotted = true;
                        continue;
                    }
                };
                let Some(entry) =
                    FooterEntry::standing_alone(key, row.lsn, row.flags, carry, value)
                else {
                    rotted = true;
                    continue;
                };
                reel.tails()[dest_index].list_carried_row(&entry)?;
                index.note_listed(&key_of, segment, row.len);
                listed += 1;
            }
        }
        Ok((listed, rotted))
    }

    #[allow(clippy::too_many_arguments)]
    fn copy_live(
        &self,
        reel: &Reel,
        dest_index: usize,
        index: &ReelIndex,
        reader: &mut SegmentReader<'_>,
        record: &SourceRecord,
        staged: Staged,
        segment: SegmentId,
    ) -> Result<CopyStep> {
        let loc = record.loc(segment);
        // The fetch phase already asked whether the index points here, so the apply asks
        // only when nothing did. The repoint below is guarded on the version the fetch
        // saw, so a record that dies between the two phases loses there.
        let staged = match staged {
            Staged::Dead => return Ok(CopyStep::Skipped),
            Staged::Live(payload) => payload,
            Staged::Unread => {
                match index.get(&record.header.key)? {
                    Some(entry) if entry.loc == loc => {}
                    Some(_) | None => return Ok(CopyStep::Skipped),
                }
                None
            }
        };

        // A record the floor has passed is dropped and its key goes with it. No
        // tombstone is written: the key is below a floor the whole volume agrees on, so
        // absence tells a later reader everything one would.
        if is_purged(reel.shared().purge_floor(), index, &record.header.key) {
            index.evict_at(&record.header.key, loc)?;
            self.metrics.record_purged(1);
            return Ok(CopyStep::Skipped);
        }

        let payload = match staged {
            Some(payload) => payload,
            None => read_payload(reader, record)?,
        };
        if !record.header.verify(&payload) {
            // With peers the eviction turns the miss into a repair enqueue. A sole copy
            // keeps its bytes where they are: rewriting them would stamp a fresh
            // checksum over rot and serve it as good.
            if reel.shared().config.repair == RepairPath::Peers {
                if index.evict_at(&record.header.key, loc)? {
                    self.metrics.record_hits(1);
                }
                return Ok(CopyStep::Skipped);
            }
            self.metrics.record_hits(1);
            tracing::warn!(
                "a record in segment {} fails its checksum on a sole copy, keeping it in place",
                segment.as_u32()
            );
            return Ok(CopyStep::Rotted);
        }

        // A value the destination's rows can hold goes into a row and nowhere else.
        // The index is not repointed: the source keeps answering until a paged search
        // finds the new row, once the destination's spans are noted at its seal.
        if let Some(entry) = self.carried_row(reel, index, dest_index, record, &payload) {
            reel.tails()[dest_index].list_carried_row(&entry)?;
            index.note_listed(&record.header.key, segment, record.header.length);
            return Ok(CopyStep::Listed);
        }

        // the queue owns what it writes and the repoint below needs the key again, so
        // the clone is required rather than incidental
        let committed = reel.tails()[dest_index].append_copy(
            record.header.key.clone(),
            record.header.lsn,
            payload,
            record.header.codec,
        )?;
        // the copy is down and the index still points at the source, which is the
        // window a concurrent delete or overwrite has to win in
        crate::sync::rendezvous::at("compaction/repoint");
        index.repoint(&record.header.key, committed.loc, record.header.lsn)?;
        Ok(CopyStep::Copied(record.span()))
    }

    /// Carry a tombstone into the destination, or drop one nothing can undo
    ///
    /// A range tombstone carries under the same rule as a point one and takes its
    /// exclusive end with it, since that is what says how far the delete reached.
    #[allow(clippy::too_many_arguments)]
    fn carry_or_drop(
        &self,
        reel: &Reel,
        dest_index: usize,
        index: &ReelIndex,
        reader: &mut SegmentReader<'_>,
        record: &SourceRecord,
        staged: Staged,
        drop_floor: Lsn,
    ) -> Result<TombstoneStep> {
        if !should_carry(index, &record.header, drop_floor)? {
            return Ok(TombstoneStep::Dropped);
        }
        let tail = &reel.tails()[dest_index];
        let carried = if record.header.flags.is_range_tombstone() {
            let end = match staged {
                Staged::Live(Some(end)) => end,
                Staged::Live(None) | Staged::Unread | Staged::Dead => read_payload(reader, record)?,
            };
            tail.append_carried_range(record.header.key.clone(), record.header.lsn, end)?
        } else {
            tail.append_carried_tombstone(record.header.key.clone(), record.header.lsn)?
        };
        // the destination has to know it holds these, or it seals with no row and the
        // delete is lost
        index.hold(&record.header.key, record.header.lsn, carried.loc);
        Ok(TombstoneStep::Carried)
    }

    /// One bounded scrub pass across the volume, resuming where the last stopped
    ///
    /// A pass reads what the rate has earned since the last one and then stops,
    /// remembering where. Running off the end starts the sweep over.
    pub fn scrub_pass(&self, reel: &Reel, index: &ReelIndex) -> Result<usize> {
        let gate = match &self.scrub_rate {
            Some(gate) => gate,
            None => return Ok(0),
        };
        // One pass at a time, tried rather than queued for, and taken before drawing on
        // the gate so the loser does not reset the running pass's clock.
        let Some(_pass) = try_lock(&self.scrub_guard) else {
            return Ok(0);
        };
        // the rate decides how much this pass may read, so the sweep runs at its
        // configured rate whether the plane is ticked once a minute or in a tight loop
        let mut budget = gate.allowance(SCRUB_PASS_MAX);
        if budget == 0 {
            return Ok(0);
        }

        let shared = reel.shared();
        let mut plan = Vec::new();
        for (segment, _bytes) in index.segments_snapshot() {
            if !shared.is_held(segment) {
                plan.push(segment);
            }
        }
        plan.sort_unstable();

        if plan.is_empty() {
            return Ok(0);
        }

        // The cursor is copied out here and written back at the pass's end, never held
        // across the sweep, so a resume-point probe does not wait out the pass's reads.
        let stored = *lock(&self.scrub_cursor);
        // A sweep that always began at the lowest segment leaves a node restarting often
        // verifying the front of the volume over and over, so a fresh sweep starts at a
        // rotated point and wraps. It is not a resume: taken as one, its first segment
        // would hand the scan a record region ending at offset zero.
        let is_resuming = stored.is_some();
        let from = stored.unwrap_or_else(|| {
            let origin = plan[(self.scrub_seed % plan.len() as u64) as usize];
            ScrubCursor {
                origin,
                segment: origin,
                offset: 0,
                region_end: 0,
                dead: 0,
            }
        });

        // turning the plan so the sweep's origin leads makes the lap a plain sequence,
        // rather than each pass setting off on a fresh lap and the sweep never ending
        let origin_at = plan.partition_point(|segment| *segment < from.origin);
        plan.rotate_left(origin_at);
        let resume_at = plan
            .iter()
            .position(|segment| *segment == from.segment)
            .unwrap_or(0);
        let mut hits = 0usize;

        for segment in &plan[resume_at..] {
            let is_carried = is_resuming && *segment == from.segment;
            let resume = is_carried.then_some((from.offset, from.region_end));
            let carried = match is_carried {
                true => from.dead,
                false => 0,
            };
            // the moment a pass is mid-sweep, for a test probing what a second caller sees
            crate::sync::rendezvous::at("scrub/segment");
            let step = self.scrub_segment(shared, index, *segment, resume, carried, &mut budget)?;
            hits += step.hits;
            if let Some((offset, region_end)) = step.resume_at {
                *lock(&self.scrub_cursor) = Some(ScrubCursor {
                    origin: from.origin,
                    segment: *segment,
                    offset,
                    region_end,
                    dead: step.dead,
                });
                return Ok(hits);
            }
            // the sweep reached this segment's end, so its tally is the whole of what
            // the index no longer resolves into it
            index.settle_dead(*segment, step.dead);
        }

        *lock(&self.scrub_cursor) = None;
        Ok(hits)
    }

    fn scrub_segment(
        &self,
        shared: &Arc<ReelShared>,
        index: &ReelIndex,
        segment: SegmentId,
        resume: Option<(u64, u64)>,
        carried: u64,
        budget: &mut u64,
    ) -> Result<ScrubStep> {
        // a sweep runs behind whatever the plane is retiring, so a segment can go
        // between the plan and the pass
        let handle = match source_handle(shared, segment) {
            Ok(handle) => handle,
            Err(error) if is_missing(&error) => {
                return Ok(ScrubStep {
                    hits: 0,
                    dead: carried,
                    resume_at: None,
                })
            }
            Err(error) => return Err(error),
        };
        let file_len = match segment_len(shared, &handle)? {
            Some(len) => len,
            None => {
                return Ok(ScrubStep {
                    hits: 0,
                    dead: carried,
                    resume_at: None,
                })
            }
        };
        let (from, region_end) = match resume {
            Some(carried) => carried,
            None => (0, footer_bound(shared, &handle, file_len)?),
        };
        let mut reader = SegmentReader::new(&shared.driver, handle.file(), region_end);
        let mut scan = RecordScan::resuming(&mut reader, from);

        let mut hits = 0usize;
        let mut dead = carried;
        loop {
            if *budget == 0 {
                self.metrics.record_hits(hits as u64);
                return Ok(ScrubStep {
                    hits,
                    dead,
                    resume_at: Some((scan.offset(), region_end)),
                });
            }
            let record = match scan.next_record()? {
                Some(record) => record,
                None => break,
            };
            *budget = budget.saturating_sub(record.span());
            if !record.header.flags.is_data() {
                continue;
            }
            let loc = record.loc(segment);
            match index.get(&record.header.key)? {
                Some(entry) if entry.loc == loc => {}
                // the index resolves this key elsewhere or nowhere, so what sits here
                // is shadowed
                Some(_) | None => {
                    dead += record.span();
                    continue;
                }
            }
            let payload = scan
                .reader()
                .range(record.payload_at(), record.header.length as usize)?;
            let is_intact =
                payload.len() == record.header.length as usize && record.header.verify(payload);
            if !is_intact {
                // With peers the eviction is the repair enqueue. A sole copy keeps the
                // key resolving so every read reports the loss.
                match shared.config.repair {
                    RepairPath::Peers => {
                        if index.evict_at(&record.header.key, loc)? {
                            hits += 1;
                        }
                    }
                    RepairPath::None => {
                        hits += 1;
                        tracing::warn!(
                            "the scrub found a record in segment {} failing its checksum on a sole copy, keeping it",
                            segment.as_u32()
                        );
                    }
                }
            }
        }
        self.metrics.record_hits(hits as u64);
        Ok(ScrubStep {
            hits,
            dead,
            resume_at: None,
        })
    }
}

/// One footer's records in key order, per column, as offset and length pairs
///
/// The order a rewrite applies its copies in, and the order the sortedness of a segment
/// is read off. Nothing where a row will not decode: the records behind such a footer
/// are still there to be scanned, so the order is given up rather than the pass.
fn footer_order(footer: &SegmentFooter) -> Option<(Vec<(u32, u32)>, u64)> {
    let mut order = Vec::with_capacity(footer.entry_count());
    let mut widest = 0usize;
    for partition in &footer.partitions {
        widest = widest.max(partition.widest_key());
        for row in 0..partition.len() {
            match partition.row_at(row) {
                // A row that stands alone names no record, and its sentinel offset
                // sitting in key order among real ones would read as unsorted for
                // ever, so every destination would re-select itself.
                Ok(row) if row.stands_alone() => {}
                Ok(row) => order.push((row.offset, row.len)),
                Err(_) => return None,
            }
        }
    }
    Some((order, HEADER_LEN as u64 + widest as u64))
}

/// Everything about a sealed segment that follows from its footer alone
///
/// A footer whose rows will not decode names no order at all, and a rewrite for order
/// has nothing to put such a segment into, so it is left where a scan can still read it.
fn footer_facts(footer: &SegmentFooter) -> FooterFacts {
    let is_out_of_order = footer_order(footer)
        .is_some_and(|(order, _)| !order.windows(2).all(|pair| pair[0].0 <= pair[1].0));
    FooterFacts {
        is_sorted_run: footer.partitions.iter().all(FooterPartition::is_sorted_run),
        is_out_of_order,
    }
}

/// Whether a tombstone has to come across, or nothing it shadows can still turn up
///
/// A newer entry in the index says the key is live again, so the delete is finished.
/// Otherwise the floor decides: a tombstone at or above it is still holding its key's
/// place against a record that has not landed yet.
fn should_carry(index: &ReelIndex, tombstone: &RecordHeader, drop_floor: Lsn) -> Result<bool> {
    if tombstone.flags.is_tombstone() {
        if let Some(entry) = index.get(&tombstone.key)? {
            if entry.lsn > tombstone.lsn {
                return Ok(false);
            }
        }
    }
    Ok(tombstone.lsn >= drop_floor)
}

impl Compactor {
    /// The tier one pass's survivors land in
    ///
    /// Demotion is one-way: a capacity segment's records stay on capacity, and a fast
    /// segment's survivors demote once enough later ingest has passed them and a
    /// capacity volume stands ready.
    fn output_class(&self, shared: &ReelShared, segment: SegmentId) -> VolumeClass {
        let source = shared.volumes.class_of(shared.volumes.root_of(segment));
        if source == VolumeClass::Capacity {
            return VolumeClass::Capacity;
        }
        if !shared.volumes.has_capacity() {
            return VolumeClass::Fast;
        }
        let Some(threshold) = self.demote_after_bytes else {
            return VolumeClass::Fast;
        };
        let behind = u64::from(shared.segment_head().saturating_sub(segment.as_u32()));
        match behind * shared.config.segment_bytes.to_bytes() >= threshold {
            true => VolumeClass::Capacity,
            false => VolumeClass::Fast,
        }
    }
}

/// Whether the purge floor has passed this key, so its record is finished
fn is_purged(floor: u64, index: &ReelIndex, key: &RecordKey) -> bool {
    if floor == NOTHING_PURGED {
        return false;
    }
    match index
        .spec(key.column)
        .and_then(|spec| spec.mark_of(key.as_slice()))
    {
        Some(mark) => mark < floor,
        None => false,
    }
}

/// The tail this pass copies its survivors into
fn destination(reel: &Reel, band: Option<Band>) -> Result<usize> {
    // A volume that rewrites at seal keeps a tail back for exactly this, since a run
    // copied in key order stops being one the moment a foreground put lands inside it.
    if let Some(reserved) = reel.reserved_tail() {
        return Ok(reserved);
    }
    // Otherwise the survivors route the way a fresh write of the same band would: into
    // the tail that band is on, and into the least loaded unbanded tail where the
    // source carried no band at all.
    reel.place(band)
}

/// The band a sealed segment was drawn under, read off its own header record
///
/// One small read at the head of a pass that is about to read the whole file, so a
/// band comes from the segment rather than from a table a restart would lose. A file
/// whose head does not read as a segment header answers nothing: the pass that follows
/// is the one that decides what to do about it.
fn band_of(shared: &Arc<ReelShared>, source: &SegmentHandle) -> Result<Option<Band>> {
    let head = shared.driver.pread(source.file(), 0, HEADER_LEN as u64)?;
    if head.len() < HEADER_LEN {
        return Ok(None);
    }
    let Ok(header) = RecordHeader::unpack(&head) else {
        return Ok(None);
    };
    if !header.flags.is_segment_header() {
        return Ok(None);
    }
    let payload =
        shared
            .driver
            .pread(source.file(), HEADER_LEN as u64, u64::from(header.length))?;
    if payload.len() < header.length as usize || !header.verify(&payload) {
        return Ok(None);
    }
    Ok(SegmentHeader::unpack(&payload)
        .map(|parsed| parsed.band)
        .unwrap_or_default())
}

/// A handle on one sealed segment, from the descriptor cache or a fresh open
pub fn source_handle(shared: &Arc<ReelShared>, segment: SegmentId) -> Result<SegmentHandle> {
    // A closed descriptor answers every op with not-found, which reads exactly like a
    // segment somebody unlinked, so asking its length here turns a stale handle into a
    // reopen rather than into a retirement that never happens.
    if let Some(handle) = shared.fd_cache.get(segment) {
        match shared.driver.length(handle.file()) {
            Ok(_) => return Ok(handle),
            Err(error) if is_missing(&error) => {
                shared.fd_cache.remove(segment);
            }
            Err(error) => return Err(error),
        }
    }
    let path = shared.segment_path(segment);
    let file = shared.driver.open(&path, false)?;
    let handle = SegmentHandle::new(segment, path, file, Arc::clone(&shared.driver));
    shared.fd_cache.insert(handle.clone());
    Ok(handle)
}

/// Length of a segment the caller already holds open
///
/// Asking the descriptor is one call, where asking the directory would be one per file
/// in it on every segment a pass visits.
pub fn segment_len(shared: &Arc<ReelShared>, handle: &SegmentHandle) -> Result<Option<u64>> {
    match shared.driver.length(handle.file()) {
        Ok(len) => Ok(Some(len)),
        Err(error) if is_missing(&error) => Ok(None),
        Err(error) => Err(error),
    }
}

/// Whether an error is a file that is not there rather than a device refusing
pub fn is_missing(error: &crate::error::ReelError) -> bool {
    if let crate::error::ReelError::Io(source) = error {
        return source.kind() == std::io::ErrorKind::NotFound;
    }
    false
}

/// Where a sealed segment's record region ends, which is where its footer begins
pub fn footer_bound(
    shared: &Arc<ReelShared>,
    handle: &SegmentHandle,
    file_len: u64,
) -> Result<u64> {
    let min_footer = FIXED_TAIL_LEN as u64;
    if file_len < min_footer {
        return Ok(file_len);
    }
    let trailer = shared
        .driver
        .pread(handle.file(), file_len - TRAILER_LEN, TRAILER_LEN)?;
    if (trailer.len() as u64) < TRAILER_LEN {
        return Ok(file_len);
    }
    let footer_len = u64::from(read_u32_le(&trailer[0..4]));
    if footer_len < min_footer || footer_len > file_len {
        return Ok(file_len);
    }
    let footer_bytes = shared
        .driver
        .pread(handle.file(), file_len - footer_len, footer_len)?;
    match SegmentFooter::parse(&footer_bytes) {
        Ok(_) => Ok(file_len - footer_len),
        Err(_) => Ok(file_len),
    }
}

/// A forward walk over one segment's records, buying its bytes in chunks
///
/// One record at a time rather than a list of all of them, since collecting a large
/// segment's headers first would cost tens of megabytes of transient memory.
pub struct RecordScan<'reader, 'driver> {
    reader: &'reader mut SegmentReader<'driver>,
    offset: u64,
}

impl<'reader, 'driver> RecordScan<'reader, 'driver> {
    /// A walk starting at a record boundary an earlier walk stopped on
    pub fn resuming(
        reader: &'reader mut SegmentReader<'driver>,
        offset: u64,
    ) -> RecordScan<'reader, 'driver> {
        RecordScan { reader, offset }
    }

    /// The reader the walk is stepping, for the payload behind a header
    pub fn reader(&mut self) -> &mut SegmentReader<'driver> {
        self.reader
    }

    /// The boundary the next record starts at
    pub fn offset(&self) -> u64 {
        self.offset
    }

    /// The next record worth visiting, or nothing once the walk runs out
    ///
    /// Anything the walk cannot make sense of ends it, since the records after it are
    /// no longer where a span says.
    pub fn next_record(&mut self) -> Result<Option<SourceRecord>> {
        let region_end = self.reader.limit();
        while self.offset + HEADER_LEN as u64 <= region_end {
            let offset = self.offset;
            let head = self.reader.range(offset, HEADER_LEN)?;
            if head.len() < HEADER_LEN {
                return Ok(None);
            }
            let width = match peek_key_width(head) {
                Some(width) => width,
                None => return Ok(None),
            };
            let prefix = self.reader.range(offset, HEADER_LEN + width)?;
            if prefix.len() < HEADER_LEN + width {
                return Ok(None);
            }
            let header = match RecordHeader::unpack(prefix) {
                Ok(header) => header,
                Err(_) => return Ok(None),
            };
            if header.is_unwritten() || !header.fits_within(region_end - offset) {
                return Ok(None);
            }
            self.offset = offset + header.span();
            if header.flags.is_data()
                || header.flags.is_tombstone()
                || header.flags.is_range_tombstone()
            {
                return Ok(Some(SourceRecord {
                    header,
                    offset: offset as u32,
                }));
            }
        }
        Ok(None)
    }
}

/// The payload behind one record header, copied out of the reader's window
pub fn read_payload(reader: &mut SegmentReader<'_>, record: &SourceRecord) -> Result<Vec<u8>> {
    let length = record.header.length as usize;
    let bytes = reader.range(record.payload_at(), length)?;
    Ok(bytes.to_vec())
}

/// Where the stripe opening at this position closes, bounded by staged bytes
///
/// The bound counts every row's length, dead rows included, which only makes stripes
/// smaller. One record always advances, so a record wider than the cap gets its own.
fn stripe_end(order: &[(u32, u32)], start: usize) -> usize {
    let mut end = start + 1;
    let mut staged = u64::from(order[start].1);
    while end < order.len() {
        let next = u64::from(order[end].1);
        if staged + next > STRIPE_STAGE_BYTES {
            break;
        }
        staged += next;
        end += 1;
    }
    end
}

/// What one punch pass over the volume found and gave back
///
/// On anything but linux the punch itself is skipped and the erased count is what the
/// pass would have punched, so the report still prices the layout.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct EraseReport {
    /// Sealed segments the pass examined
    pub segments: u64,

    /// Bytes sitting in dead data-record runs, before block alignment
    pub dead_run_bytes: u64,

    /// Bytes of whole filesystem blocks inside those runs, punched on linux
    pub erased_bytes: u64,
}

/// Bytes a filesystem block holds, the granularity a punch can act at
const ERASE_BLOCK: u64 = 4096;

/// Give a range's blocks back to the filesystem, keeping the file's length
#[cfg(target_os = "linux")]
fn erase_range(file: &std::fs::File, offset: u64, len: u64) -> Result<()> {
    use std::os::fd::AsRawFd;
    let mode = libc::FALLOC_FL_PUNCH_HOLE | libc::FALLOC_FL_KEEP_SIZE;
    // SAFETY: an ffi call against a descriptor the caller holds open across it.
    let ret = unsafe {
        libc::fallocate(
            file.as_raw_fd(),
            mode,
            offset as libc::off_t,
            len as libc::off_t,
        )
    };
    match ret {
        0 => Ok(()),
        _ => Err(std::io::Error::last_os_error().into()),
    }
}

/// The punch is a linux call; elsewhere the pass only reports what it found
#[cfg(not(target_os = "linux"))]
fn erase_range(_file: &std::fs::File, _offset: u64, _len: u64) -> Result<()> {
    Ok(())
}

/// One coalesced read covering a run of a stripe's records
struct ChunkRange {
    /// Offset in the segment the read begins at
    start: u64,

    /// Bytes the read covers
    len: usize,

    /// The stretch of the offset-sorted plan this range serves
    members: std::ops::Range<usize>,
}

/// Ranges the next fetch wave takes from what is left of a stripe's chunking
///
/// The whole batch door unpaced, which is the shape the ring was sized for. A paced
/// pass stops at the first range that fills its step, since a wave wider than a step is
/// device time the gate has no way to interrupt. One range is the floor: the chunking
/// has already cut at the reader's chunk and a lower cap cannot have less.
fn wave_len(ranges: &[ChunkRange], step_bytes: u64) -> usize {
    let depth = ranges.len().min(FETCH_DEPTH);
    if step_bytes == 0 {
        return depth;
    }
    let mut bytes = 0u64;
    for (taken, range) in ranges.iter().take(depth).enumerate() {
        bytes += range.len as u64;
        if bytes >= step_bytes {
            return taken + 1;
        }
    }
    depth
}

/// Coalesce a stripe's offset-sorted records into ranged reads
///
/// Consecutive records pack into one range up to the reader's chunk, so a dense stripe
/// reads as sequential chunks and a sparse one skips the dead between its survivors.
/// The span behind each offset is a hint, and a record wider than its hint is served by
/// the window's own refill.
fn chunk_ranges(
    plan: &[(usize, u32)],
    order: &[(u32, u32)],
    prefix_hint: u64,
    region_end: u64,
) -> Vec<ChunkRange> {
    let mut ranges = Vec::new();
    let mut begin = 0usize;
    while begin < plan.len() {
        let start = u64::from(plan[begin].1);
        let mut reach = start;
        let mut end_at = begin;
        while end_at < plan.len() {
            let (position, offset) = plan[end_at];
            let record_end =
                (u64::from(offset) + prefix_hint + u64::from(order[position].1)).min(region_end);
            if end_at > begin && record_end.saturating_sub(start) > READ_CHUNK as u64 {
                break;
            }
            reach = reach.max(record_end);
            end_at += 1;
        }
        ranges.push(ChunkRange {
            start,
            len: reach.saturating_sub(start) as usize,
            members: begin..end_at,
        });
        begin = end_at;
    }
    ranges
}

#[cfg(test)]
mod tests {
    use super::*;

    use reel_core::Value;

    use std::path::{Path, PathBuf};
    use std::sync::Barrier;
    use std::thread;
    use std::time::Instant;

    use crate::units::ByteCount;

    use crate::append::admission::InflightBudget;
    use crate::append::Commit;
    use crate::config::{
        CompactRate, Preallocate, ReelConfig, SyncPolicy, ThreadBudget, DEFAULT_FD_CACHE,
    };
    use crate::format::column::{
        Codec, ColumnId, ColumnSet, ColumnSpec, KeyWidth, MapShape, PurgeMark, RecordKey,
    };
    use crate::format::segment_header::SEGMENT_HEADER_SPAN;
    use crate::index::entry::span_of;
    use crate::index::recovery::{rebuild_reel, RebuiltReel};
    use crate::io::fault::{FaultKind, FaultPlan};
    use crate::io::sim_backend::{DurableImage, SimIo};
    use crate::reel::segment::{FdCache, IoDriver};
    use crate::reel::segment_file_name;
    use crate::reel::RecordRead;

    const REEL_DIR: &str = "/bulk";
    const RECORDS: ColumnId = ColumnId(1);
    const KEY_WIDTH: usize = 34;
    const SEG_HEADER_SPAN: usize = HEADER_LEN + SEGMENT_HEADER_SPAN;

    const COLUMNS: ColumnSet = &[ColumnSpec {
        id: RECORDS,
        name: "records",
        key_width: KeyWidth::Fixed(KEY_WIDTH as u16),
        shard_bytes: 2,
        inline_max: 0,
        row_carry: 0,
        purge_mark: None,
        codec: Codec::None,
        map_shape: MapShape::Tree,
    }];

    /// Bytes a marked key takes: the mark, then an index within it
    const MARKED_WIDTH: usize = 16;

    const MARKED: ColumnSet = &[ColumnSpec {
        id: RECORDS,
        name: "marked",
        key_width: KeyWidth::Fixed(MARKED_WIDTH as u16),
        shard_bytes: 0,
        inline_max: 0,
        row_carry: 0,
        purge_mark: Some(PurgeMark::at(0)),
        codec: Codec::None,
        map_shape: MapShape::Tree,
    }];

    fn marked_key(mark: u64, index: u64) -> RecordKey {
        let mut bytes = [0u8; MARKED_WIDTH];
        bytes[..8].copy_from_slice(&mark.to_be_bytes());
        bytes[8..].copy_from_slice(&index.to_be_bytes());
        RecordKey::from_bytes(RECORDS, &bytes).expect("key")
    }

    struct Fixture {
        sim: SimIo,
        reel: Reel,
        index: ReelIndex,
        compactor: Compactor,
    }

    fn settings() -> ReelConfig {
        ReelConfig {
            segment_bytes: ByteCount::mb(1),
            alloc_chunk: ByteCount::from_bytes(16_384),
            preallocate: Preallocate::Chunk,
            sync: SyncPolicy::Never,
            active_tails: ThreadBudget::threads(1),
            scrub_mbps: 4,
            ..ReelConfig::default()
        }
    }

    fn fixture(config: ReelConfig) -> Fixture {
        fixture_over(config, COLUMNS)
    }

    fn fixture_over(config: ReelConfig, columns: ColumnSet) -> Fixture {
        let sim = SimIo::new(FaultPlan::new(1));
        let driver = Arc::new(IoDriver::new(Arc::new(sim.clone())));
        let budget = Arc::new(InflightBudget::default());
        let fd_cache = Arc::new(FdCache::new(DEFAULT_FD_CACHE as usize));
        let shared = Arc::new(ReelShared::new(
            PathBuf::from(REEL_DIR),
            driver,
            budget,
            fd_cache,
            config.clone(),
            columns,
            1,
        ));
        let reel = Reel::open(Arc::clone(&shared)).expect("open reel");
        let index = ReelIndex::new(
            columns,
            crate::config::IndexResidency::Resident,
            crate::config::ShardShapes::Tree,
        )
        .expect("index");
        // What the engine wires at open: the seal reads a segment's tally from these
        // and a landing books its floor into them.
        shared.set_segments(index.segments_handle());
        let compactor = Compactor::new(&config, 0, 0);
        Fixture {
            sim,
            reel,
            index,
            compactor,
        }
    }

    fn key(byte: u8) -> RecordKey {
        RecordKey::from_bytes(RECORDS, &[byte; KEY_WIDTH]).expect("key")
    }

    fn put(fixture: &Fixture, byte: u8, payload: Vec<u8>) {
        let committed = fixture
            .reel
            .put(key(byte), payload, 0, Commit::PerRecord)
            .expect("put");
        fixture
            .index
            .insert(&key(byte), committed.loc, committed.lsn, None)
            .expect("insert");
    }

    fn delete(fixture: &Fixture, byte: u8) {
        let committed = fixture
            .reel
            .delete(key(byte), Commit::PerRecord)
            .expect("delete");
        fixture
            .index
            .remove(&key(byte), committed.lsn, committed.loc)
            .expect("remove");
    }

    /// Seal the tail and take the segment off the sealed queue
    ///
    /// These fixtures hold a reel and an index with no engine between them, and a
    /// segment still owed its spans is left alone by compaction.
    fn seal(fixture: &Fixture) {
        fixture.reel.tails()[0].seal().expect("seal");
        let shared = fixture.reel.shared();
        let owed = shared.pending_seals();
        shared.settle_sealed(&owed);
    }

    fn seg_path(number: u32) -> PathBuf {
        Path::new(REEL_DIR).join(segment_file_name(SegmentId(number)))
    }

    fn rebuilt_from(fixture: &Fixture) -> RebuiltReel {
        let image = fixture.sim.durable_image();
        let restored = SimIo::from_image(image);
        let driver = IoDriver::new(Arc::new(restored));
        rebuild_reel(&driver, &[PathBuf::from(REEL_DIR)], &[false], false).expect("rebuild")
    }

    fn reopen(image: DurableImage, config: ReelConfig) -> Fixture {
        let restored = SimIo::from_image(image);
        let sim = restored.clone();
        let driver = Arc::new(IoDriver::new(Arc::new(restored)));
        let budget = Arc::new(InflightBudget::default());
        let fd_cache = Arc::new(FdCache::new(DEFAULT_FD_CACHE as usize));
        let shared = Arc::new(ReelShared::new(
            PathBuf::from(REEL_DIR),
            Arc::clone(&driver),
            budget,
            fd_cache,
            config.clone(),
            COLUMNS,
            1,
        ));
        let rebuilt = rebuild_reel(driver.as_ref(), &[PathBuf::from(REEL_DIR)], &[false], false)
            .expect("rebuild");
        shared.lsn.recover_to(rebuilt.highest_lsn);
        shared.recover_next_segment(rebuilt.highest_segment);
        let index = ReelIndex::new(
            COLUMNS,
            crate::config::IndexResidency::Resident,
            crate::config::ShardShapes::Tree,
        )
        .expect("index");
        index.install(
            rebuilt.entries,
            rebuilt.covers,
            rebuilt.segments,
            rebuilt.segment_min_lsn,
            rebuilt.segment_max_lsn,
            rebuilt.sealed,
            rebuilt.sealed_keys,
        );
        let reel = Reel::open(Arc::clone(&shared)).expect("reopen reel");
        let compactor = Compactor::new(&config, 0, 0);
        Fixture {
            sim,
            reel,
            index,
            compactor,
        }
    }

    /// Keys the rebuild resolved in the record column, in key order
    fn rebuilt_keys(rebuilt: &RebuiltReel) -> Vec<Vec<u8>> {
        let mut out: Vec<Vec<u8>> = rebuilt
            .entries
            .get(&RECORDS)
            .map(|rows| {
                rows.iter()
                    .map(|(key, _)| key.as_slice().to_vec())
                    .collect()
            })
            .unwrap_or_default();
        out.sort();
        out
    }

    /// The bytes of one key, for comparing against a rebuild
    fn key_bytes(byte: u8) -> Vec<u8> {
        vec![byte; KEY_WIDTH]
    }

    // a segment past the threshold is rewritten and retired after the copies are durable
    #[test]
    fn rewrites_and_retires() {
        let fixture = fixture(settings());
        put(&fixture, 1, vec![0x11; 200]);
        put(&fixture, 2, vec![0x22; 200]);
        seal(&fixture);
        put(&fixture, 1, vec![0x33; 300]);

        fixture
            .compactor
            .compact_segment(&fixture.reel, &fixture.index, SegmentId(1))
            .expect("compact");
        fixture.reel.flush().expect("flush");

        let live_two = fixture
            .index
            .get(&key(2))
            .expect("read")
            .expect("two present");
        assert_ne!(live_two.loc.segment, SegmentId(1));
        assert_eq!(
            fixture
                .index
                .get(&key(1))
                .expect("read")
                .expect("one present")
                .lsn,
            Lsn(3)
        );

        assert!(fixture.sim.durable_bytes(&seg_path(1)).is_none());
        let payload = fixture
            .reel
            .read_record(live_two.loc, key(2).as_ref(), live_two.lsn, true)
            .expect("read");
        assert_eq!(payload, RecordRead::Found(Value::new(vec![0x22; 200])));

        let counters = fixture.compactor.counters();
        assert_eq!(counters.segments_rewritten, 1);
        assert!(counters.compaction_bytes > 0);

        let rebuilt = rebuilt_from(&fixture);
        assert_eq!(rebuilt_keys(&rebuilt), vec![key_bytes(1), key_bytes(2)]);
    }

    // a record the purge floor has passed is dropped rather than copied forward
    #[test]
    fn purged_records_are_not_copied() {
        let fixture = fixture_over(settings(), MARKED);
        for mark in 1..=6u64 {
            let key = marked_key(mark, 0);
            let committed = fixture
                .reel
                .put(key.clone(), vec![mark as u8; 200], 0, Commit::PerRecord)
                .expect("put");
            fixture
                .index
                .insert(&key, committed.loc, committed.lsn, None)
                .expect("insert");
        }
        seal(&fixture);

        fixture.reel.shared().purge_below(4);
        fixture
            .compactor
            .compact_segment(&fixture.reel, &fixture.index, SegmentId(1))
            .expect("compact");
        fixture.reel.flush().expect("flush");

        for mark in 1..=3u64 {
            assert!(
                fixture
                    .index
                    .get(&marked_key(mark, 0))
                    .expect("read")
                    .is_none(),
                "a key below the floor is gone"
            );
        }
        for mark in 4..=6u64 {
            let entry = fixture
                .index
                .get(&marked_key(mark, 0))
                .expect("read")
                .expect("kept");
            assert_ne!(entry.loc.segment, SegmentId(1), "and was copied forward");
        }
        assert_eq!(fixture.compactor.counters().records_purged, 3);
    }

    // a column that marks no key is untouched however far the floor moves
    #[test]
    fn unmarked_column_ignores_the_floor() {
        let fixture = fixture(settings());
        put(&fixture, 1, vec![0x11; 200]);
        put(&fixture, 2, vec![0x22; 200]);
        seal(&fixture);

        fixture.reel.shared().purge_below(u64::MAX);
        fixture
            .compactor
            .compact_segment(&fixture.reel, &fixture.index, SegmentId(1))
            .expect("compact");
        fixture.reel.flush().expect("flush");

        assert!(fixture.index.get(&key(1)).expect("read").is_some());
        assert!(fixture.index.get(&key(2)).expect("read").is_some());
        assert_eq!(fixture.compactor.counters().records_purged, 0);
    }

    // the floor only ever moves up, so a stale setter cannot un-purge
    #[test]
    fn the_floor_only_rises() {
        let fixture = fixture_over(settings(), MARKED);

        fixture.reel.shared().purge_below(10);
        fixture.reel.shared().purge_below(4);

        assert_eq!(fixture.reel.shared().purge_floor(), 10);
    }

    // a fully dead segment is unlinked whole with no rewrite
    #[test]
    fn unlinks_whole_when_dead() {
        let fixture = fixture(settings());
        put(&fixture, 1, vec![0x11; 200]);
        seal(&fixture);
        put(&fixture, 1, vec![0x33; 200]);

        fixture
            .compactor
            .compact_segment(&fixture.reel, &fixture.index, SegmentId(1))
            .expect("compact");

        let counters = fixture.compactor.counters();
        assert_eq!(counters.segments_unlinked_whole, 1);
        assert_eq!(counters.segments_rewritten, 0);
        assert_eq!(counters.compaction_bytes, 0);
        assert_eq!(counters.move_ratio(), 0.0);
        assert!(fixture.sim.durable_bytes(&seg_path(1)).is_none());
    }

    // retiring a segment is paced by its scan even when nothing is copied
    #[test]
    fn a_retirement_charges_its_scan() {
        let slow = ReelConfig {
            compact_mbps: CompactRate::Mbps(1),
            ..settings()
        };
        let fixture = fixture(slow);
        put(&fixture, 1, vec![0x11; 200_000]);
        seal(&fixture);
        put(&fixture, 1, vec![0x33; 200]);

        assert!(
            fixture.compactor.is_compaction_due(),
            "a fresh gate is open"
        );
        let started = Instant::now();
        fixture
            .compactor
            .compact_segment(&fixture.reel, &fixture.index, SegmentId(1))
            .expect("compact");
        let elapsed = started.elapsed();

        let counters = fixture.compactor.counters();
        assert_eq!(counters.segments_unlinked_whole, 1);
        assert_eq!(counters.compaction_bytes, 0, "nothing was copied");
        assert!(
            elapsed >= Duration::from_millis(150),
            "the scan's reads left the pass free to run, {elapsed:?}",
        );
    }

    // key order running against offset order is fetched forward, not a refill each
    #[test]
    fn a_reversed_key_order_reads_the_region_once() {
        let wide = ReelConfig {
            segment_bytes: ByteCount::mb(8),
            ..settings()
        };
        let fixture = fixture(wide);
        for byte in (0..230u8).rev() {
            put(&fixture, byte, vec![byte; 32 * 1024]);
        }
        seal(&fixture);
        let file_len = fixture
            .sim
            .durable_bytes(&seg_path(1))
            .expect("sealed segment")
            .len() as u64;

        fixture
            .compactor
            .compact_segment(&fixture.reel, &fixture.index, SegmentId(1))
            .expect("compact");
        fixture.reel.flush().expect("flush");

        for byte in [0u8, 229u8] {
            let entry = fixture.index.get(&key(byte)).expect("read").expect("kept");
            assert_ne!(
                entry.loc.segment,
                SegmentId(1),
                "the record was copied forward"
            );
        }

        let counters = fixture.compactor.counters();
        assert!(counters.read_bytes > 0, "the pass counted its reads");
        assert!(
            counters.read_bytes < 3 * file_len,
            "a reversed order read {} bytes against a {} byte region",
            counters.read_bytes,
            file_len,
        );
    }

    // a drained volume reports idle rather than claiming work for ever
    #[test]
    fn a_drained_volume_goes_idle() {
        let fixture = fixture(settings());
        for byte in 0..8u8 {
            put(&fixture, byte, vec![byte; 4096]);
        }
        seal(&fixture);
        for byte in 0..8u8 {
            put(&fixture, byte, vec![byte ^ 0xff; 4096]);
        }
        fixture.reel.flush().expect("flush");

        // bounded, so a plane that never settles fails here instead of hanging
        let mut verdicts = Vec::new();
        for _ in 0..64 {
            let before = fixture.index.dead_bytes();
            let target = fixture
                .compactor
                .select_target(&fixture.reel, &fixture.index, 0.5, None);
            match target {
                Some((segment, _)) => {
                    fixture
                        .compactor
                        .compact_segment(&fixture.reel, &fixture.index, segment)
                        .expect("compact");
                    verdicts.push(fixture.index.dead_bytes() < before);
                }
                None => {
                    verdicts.push(false);
                    break;
                }
            }
        }

        assert!(
            verdicts.last() == Some(&false),
            "the plane never ran out of work to claim",
        );
        assert!(
            fixture
                .compactor
                .select_target(&fixture.reel, &fixture.index, 0.5, None)
                .is_none(),
            "a drained volume still offers a target",
        );
    }

    // a repoint loses to a concurrent overwrite and the copy is booked dead
    #[test]
    fn overwrite_wins_repoint() {
        let fixture = fixture(settings());
        put(&fixture, 1, vec![0x11; 200]);
        seal(&fixture);

        let shared = fixture.reel.shared();
        let source = source_handle(shared, SegmentId(1)).expect("handle");
        let file_len = segment_len(shared, &source).expect("len").expect("present");
        let region_end = footer_bound(shared, &source, file_len).expect("bound");
        let mut reader = SegmentReader::new(&shared.driver, source.file(), region_end);
        let mut scan = RecordScan::resuming(&mut reader, 0);
        let record = loop {
            let record = scan.next_record().expect("scan").expect("data record");
            if record.header.flags.is_data() {
                break record;
            }
        };
        let payload = read_payload(scan.reader(), &record).expect("payload");

        put(&fixture, 1, vec![0x44; 500]);
        let committed = fixture.reel.tails()[0]
            .append_copy(
                record.header.key,
                record.header.lsn,
                payload,
                record.header.codec,
            )
            .expect("copy");
        let moved = fixture
            .index
            .repoint(&key(1), committed.loc, record.header.lsn)
            .expect("repoint");

        assert!(!moved);
        assert_eq!(
            fixture
                .index
                .get(&key(1))
                .expect("read")
                .expect("present")
                .lsn,
            Lsn(2)
        );
        assert_eq!(
            fixture.index.segment_bytes(committed.loc.segment).dead,
            span_of(KEY_WIDTH as u16, 200),
        );
    }

    // a tombstone that another segment can still resurrect is carried across
    #[test]
    fn carries_shadowing_tombstone() {
        let fixture = fixture(settings());
        put(&fixture, 1, vec![0x11; 200]);
        seal(&fixture);
        put(&fixture, 2, vec![0x22; 200]);
        delete(&fixture, 1);
        put(&fixture, 2, vec![0x33; 200]);
        seal(&fixture);
        put(&fixture, 3, vec![0x44; 200]);

        fixture
            .compactor
            .compact_segment(&fixture.reel, &fixture.index, SegmentId(2))
            .expect("compact");
        fixture.reel.flush().expect("flush");

        assert_eq!(fixture.compactor.counters().tombstones_carried, 1);
        assert_eq!(fixture.compactor.counters().tombstones_dropped, 0);

        let rebuilt = rebuilt_from(&fixture);
        assert!(!rebuilt_keys(&rebuilt).contains(&key_bytes(1)));
    }

    // a tombstone below every other segment's oldest record is trimmed
    #[test]
    fn trims_superseded_tombstone() {
        let fixture = fixture(settings());
        put(&fixture, 1, vec![0x11; 200]);
        delete(&fixture, 1);
        seal(&fixture);
        put(&fixture, 2, vec![0x22; 200]);
        seal(&fixture);
        put(&fixture, 3, vec![0x33; 200]);

        fixture
            .compactor
            .compact_segment(&fixture.reel, &fixture.index, SegmentId(1))
            .expect("compact");
        fixture.reel.flush().expect("flush");

        assert_eq!(fixture.compactor.counters().tombstones_dropped, 1);
        assert_eq!(fixture.compactor.counters().tombstones_carried, 0);
        assert!(fixture.sim.durable_bytes(&seg_path(1)).is_none());

        let rebuilt = rebuilt_from(&fixture);
        assert_eq!(rebuilt_keys(&rebuilt), vec![key_bytes(2), key_bytes(3)]);
    }

    // a number drawn before a delete can still land under it, so the tombstone stands
    #[test]
    fn carries_tombstone_over_a_drawn_number() {
        let fixture = fixture(settings());
        put(&fixture, 1, vec![0x11; 200]);
        delete(&fixture, 1);
        seal(&fixture);
        put(&fixture, 2, vec![0x22; 200]);
        seal(&fixture);
        put(&fixture, 3, vec![0x33; 200]);

        // one writer past its draw and short of its segment, which is what the floor
        // the standing segments show cannot see
        let drawn = fixture.reel.shared().draw_gauge(1);
        fixture
            .compactor
            .compact_segment(&fixture.reel, &fixture.index, SegmentId(1))
            .expect("compact");
        drop(drawn);
        fixture.reel.flush().expect("flush");

        assert_eq!(fixture.compactor.counters().tombstones_carried, 1);
        assert_eq!(fixture.compactor.counters().tombstones_dropped, 0);
    }

    // a record that landed and never published still holds its segment's floor down
    #[test]
    fn a_landed_orphan_keeps_its_tombstone() {
        let fixture = fixture(settings());
        // What a dropped future leaves: the bytes are on the device under the oldest
        // number on the volume, and no index entry ever names them.
        drop(
            fixture
                .reel
                .put(key(1), vec![0x11; 200], 0, Commit::PerRecord)
                .expect("orphan"),
        );
        seal(&fixture);
        put(&fixture, 1, vec![0x22; 200]);
        delete(&fixture, 1);
        seal(&fixture);
        put(&fixture, 2, vec![0x33; 200]);

        fixture
            .compactor
            .compact_segment(&fixture.reel, &fixture.index, SegmentId(2))
            .expect("compact");
        fixture.reel.flush().expect("flush");

        assert_eq!(fixture.compactor.counters().tombstones_carried, 1);
        assert_eq!(fixture.compactor.counters().tombstones_dropped, 0);

        let rebuilt = rebuilt_from(&fixture);
        assert!(!rebuilt_keys(&rebuilt).contains(&key_bytes(1)));
    }

    // a dropped tombstone never resurrects its key on a rebuild
    #[test]
    fn dropped_tombstone_stays_deleted() {
        let fixture = fixture(settings());
        put(&fixture, 1, vec![0x11; 200]);
        delete(&fixture, 1);
        seal(&fixture);
        put(&fixture, 2, vec![0x22; 200]);
        seal(&fixture);
        put(&fixture, 3, vec![0x33; 200]);

        fixture
            .compactor
            .compact_segment(&fixture.reel, &fixture.index, SegmentId(1))
            .expect("compact");
        fixture.reel.flush().expect("flush");

        let rebuilt = rebuilt_from(&fixture);
        assert!(!rebuilt_keys(&rebuilt).contains(&key_bytes(1)));
        assert_eq!(rebuilt_keys(&rebuilt).len(), 2);
    }

    // a crash after copying but before unlinking rebuilds one version from two copies
    #[test]
    fn mid_compaction_crash_one_version() {
        let fixture = fixture(settings());
        put(&fixture, 1, vec![0x55; 300]);
        seal(&fixture);

        fixture.reel.tails()[0]
            .append_copy(key(1), Lsn(1), vec![0x55; 300], 0)
            .expect("copy");
        fixture.reel.flush().expect("flush");

        assert!(fixture.sim.durable_bytes(&seg_path(1)).is_some());

        let rebuilt = rebuilt_from(&fixture);
        assert_eq!(rebuilt_keys(&rebuilt), vec![key_bytes(1)]);
        let (_, entry) = rebuilt.entries[&RECORDS][0];
        assert_eq!(entry.lsn, Lsn(1));
    }

    // the dead-space gauge falls once a compacted segment is retired
    #[test]
    fn dead_gauge_reclaims() {
        let fixture = fixture(settings());
        put(&fixture, 1, vec![0x11; 200]);
        seal(&fixture);
        put(&fixture, 1, vec![0x33; 200]);
        let before = fixture.index.dead_bytes();

        fixture
            .compactor
            .compact_segment(&fixture.reel, &fixture.index, SegmentId(1))
            .expect("compact");

        assert!(before > 0);
        assert_eq!(fixture.index.dead_bytes(), 0);
    }

    // the automatic compaction rate is unpaced and the scrub keeps its own rate
    #[test]
    fn rate_and_scrub_toggle() {
        let running = Compactor::new(&settings(), 0, 0);
        let disabled = Compactor::new(
            &ReelConfig {
                scrub_mbps: 0,
                ..settings()
            },
            0,
            0,
        );

        assert_eq!(running.compaction_rate_mbps(), 0);
        assert!(running.is_scrub_enabled());
        assert_eq!(running.scrub_rate_mbps(), Some(4));
        assert!(!disabled.is_scrub_enabled());
        assert_eq!(disabled.scrub_rate_mbps(), None);
    }

    /// The op an out of space fault is injected at, past the open's own ops
    const ENOSPC_AT: u64 = 11;

    fn engine_config() -> ReelConfig {
        ReelConfig {
            segment_bytes: ByteCount::from_bytes(8_192),
            alloc_chunk: ByteCount::from_bytes(4_096),
            preallocate: Preallocate::Chunk,
            sync: SyncPolicy::Never,
            active_tails: ThreadBudget::threads(1),
            scrub_mbps: 4,
            ..ReelConfig::default()
        }
    }

    // a sync per put, so a scrub pass reads bytes the device already has
    fn scrub_config() -> ReelConfig {
        ReelConfig {
            sync: SyncPolicy::EveryPut,
            ..engine_config()
        }
    }

    fn engine_store(config: ReelConfig) -> (crate::engine::ReelStore, SimIo) {
        let sim = SimIo::new(FaultPlan::new(1));
        let store = crate::engine::ReelStore::open_with_io(
            PathBuf::from("/bulk"),
            config,
            COLUMNS,
            Arc::new(sim.clone()),
        )
        .expect("open store");
        (store, sim)
    }

    fn flip_first_payload(image: &mut DurableImage, path: &Path) {
        for (candidate, bytes) in image.iter_mut() {
            if candidate == path {
                let at = SEG_HEADER_SPAN + HEADER_LEN + KEY_WIDTH;
                if at < bytes.len() {
                    bytes[at] ^= 0xff;
                }
            }
        }
    }

    // a compaction pass selects the fullest segment and rewrites it end to end
    #[test]
    fn compact_once_selects_target() {
        let (store, _sim) = engine_store(engine_config());
        for byte in 1..=4u8 {
            store.put(&key(byte), &[byte; 3_000]).expect("put");
        }
        for byte in 1..=3u8 {
            store
                .put(&key(byte), &[byte + 100; 3_000])
                .expect("overwrite");
        }
        store.flush().expect("flush");

        store.compact_once().expect("compact");

        for byte in 1..=4u8 {
            assert!(store.contains(&key(byte)).expect("read"));
        }
        assert_eq!(store.totals().count, 4);
        let counters = store.compaction_counters();
        assert!(counters.segments_rewritten + counters.segments_unlinked_whole >= 1);
    }

    // demotion is owed after half the fast tier of later ingest, and never without one
    #[test]
    fn the_demotion_threshold_is_half_the_fast_tier() {
        let config = settings();

        let sized = Compactor::new(&config, 0, 8 << 30);
        assert_eq!(
            sized.demote_after_bytes,
            Some(4 << 30),
            "half the fast tier is what a segment has to fall behind",
        );

        // A tier nothing can say the size of leaves every survivor in its own class,
        // since a threshold read off nothing would demote on the first pass.
        let unknown = Compactor::new(&config, 0, 0);
        assert_eq!(unknown.demote_after_bytes, None);
    }

    // a scrub pass finds an injected bit flip in a sealed segment and evicts the key
    #[test]
    fn scrub_evicts_bit_flip() {
        let fixture = fixture(settings());
        put(&fixture, 1, vec![0x11; 200]);
        put(&fixture, 2, vec![0x22; 200]);
        seal(&fixture);
        fixture.reel.flush().expect("flush");

        let mut image = fixture.sim.durable_image();
        flip_first_payload(&mut image, &seg_path(1));
        let reopened = reopen(image, settings());

        // The scrub earns its bytes from the clock and starts wherever this process's
        // rotation puts it, so the sweep is run to completion and the hit counted over
        // the whole of it.
        let mut hits = 0;
        for _ in 0..64 {
            std::thread::sleep(Duration::from_millis(20));
            hits += reopened
                .compactor
                .scrub_pass(&reopened.reel, &reopened.index)
                .expect("scrub");
            if reopened.compactor.scrub_resume_point().is_none() && hits > 0 {
                break;
            }
        }

        assert_eq!(hits, 1);
        assert!(reopened.index.get(&key(1)).expect("read").is_none());
        assert!(reopened.index.get(&key(2)).expect("read").is_some());
        assert_eq!(reopened.compactor.counters().scrub_hits, 1);
    }

    // a scrub hit on a sole copy is counted and the key keeps resolving
    #[test]
    fn a_sole_copy_scrub_counts_and_keeps() {
        let sole = ReelConfig {
            repair: RepairPath::None,
            ..settings()
        };
        let fixture = fixture(sole.clone());
        put(&fixture, 1, vec![0x11; 200]);
        put(&fixture, 2, vec![0x22; 200]);
        seal(&fixture);
        fixture.reel.flush().expect("flush");

        let mut image = fixture.sim.durable_image();
        flip_first_payload(&mut image, &seg_path(1));
        let reopened = reopen(image, sole);

        let mut hits = 0;
        for _ in 0..64 {
            std::thread::sleep(Duration::from_millis(20));
            hits += reopened
                .compactor
                .scrub_pass(&reopened.reel, &reopened.index)
                .expect("scrub");
            if reopened.compactor.scrub_resume_point().is_none() && hits > 0 {
                break;
            }
        }

        assert_eq!(hits, 1);
        assert!(
            reopened.index.get(&key(1)).expect("read").is_some(),
            "the rotted key keeps its place, a read reports it"
        );
        assert!(reopened.index.get(&key(2)).expect("read").is_some());
    }

    // compaction leaves a rotted sole-copy segment standing rather than unlinking it
    #[test]
    fn a_sole_copy_rotted_segment_is_not_retired() {
        let sole = ReelConfig {
            repair: RepairPath::None,
            ..settings()
        };
        let fixture = fixture(sole.clone());
        for byte in 1..=3u8 {
            put(&fixture, byte, vec![byte; 4096]);
        }
        seal(&fixture);
        fixture.reel.flush().expect("flush");

        let mut image = fixture.sim.durable_image();
        flip_first_payload(&mut image, &seg_path(1));
        let reopened = reopen(image, sole);

        reopened
            .compactor
            .compact_segment(&reopened.reel, &reopened.index, SegmentId(1))
            .expect("compact");

        let rotted = reopened
            .index
            .get(&key(1))
            .expect("read")
            .expect("still resolves");
        assert_eq!(
            rotted.loc.segment,
            SegmentId(1),
            "the rotted record stays where it was"
        );
        for byte in 2..=3u8 {
            let moved = reopened
                .index
                .get(&key(byte))
                .expect("read")
                .expect("resolves");
            assert_ne!(
                moved.loc.segment,
                SegmentId(1),
                "the intact records were copied on"
            );
        }

        // a second pass finds the same rot and leaves the segment standing again
        reopened
            .compactor
            .compact_segment(&reopened.reel, &reopened.index, SegmentId(1))
            .expect("compact again");
        let still = reopened
            .index
            .get(&key(1))
            .expect("read")
            .expect("still resolves");
        assert_eq!(still.loc.segment, SegmentId(1));
    }

    // a range covers the prefix the footer declares and no more
    #[test]
    fn a_range_covers_the_prefix_the_footer_declares() {
        const KEY: u64 = 34;
        const PAYLOAD: u32 = 4096;
        // further apart than a read chunk, so nothing coalesces and every range holds one
        let order: Vec<(u32, u32)> = (0..4u32)
            .map(|at| (at * 4 * READ_CHUNK as u32, PAYLOAD))
            .collect();
        let plan: Vec<(usize, u32)> = order
            .iter()
            .enumerate()
            .map(|(at, &(offset, _))| (at, offset))
            .collect();

        let ranges = chunk_ranges(&plan, &order, HEADER_LEN as u64 + KEY, u64::MAX);

        assert_eq!(ranges.len(), order.len(), "sparse records coalesced");
        for range in &ranges {
            assert_eq!(
                range.len as u64,
                HEADER_LEN as u64 + KEY + u64::from(PAYLOAD),
                "the range reads past the record it is for",
            );
        }
    }

    // a paced wave stops at the step, an unpaced one takes the whole batch door
    #[test]
    fn a_wave_is_cut_at_the_step() {
        const RANGE: usize = 4096;
        let ranges: Vec<ChunkRange> = (0..FETCH_DEPTH + 2)
            .map(|at| ChunkRange {
                start: (at * RANGE) as u64,
                len: RANGE,
                members: at..at + 1,
            })
            .collect();

        assert_eq!(
            wave_len(&ranges, 0),
            FETCH_DEPTH,
            "unpaced took less than the door"
        );
        assert_eq!(wave_len(&ranges, RANGE as u64), 1);
        assert_eq!(wave_len(&ranges, 3 * RANGE as u64), 3);
        // a step no range fits under still takes one, and one wider than the door does
        // not widen it
        assert_eq!(wave_len(&ranges, 1), 1);
        assert_eq!(wave_len(&ranges, u64::MAX), FETCH_DEPTH);
    }

    // a paced pass holds itself to its rate while it copies, not only after
    #[test]
    fn a_copying_pass_paces_itself() {
        let slow = ReelConfig {
            compact_mbps: CompactRate::Mbps(1),
            ..settings()
        };
        let fixture = fixture(slow);
        for byte in 0..4u8 {
            put(&fixture, byte, vec![byte; 50_000]);
        }
        seal(&fixture);
        // two of the four go dead, so the pass has both copies and skips to pace
        put(&fixture, 0, vec![0x33; 200]);
        put(&fixture, 1, vec![0x33; 200]);

        let started = Instant::now();
        fixture
            .compactor
            .compact_segment(&fixture.reel, &fixture.index, SegmentId(1))
            .expect("compact");
        let elapsed = started.elapsed();

        assert_eq!(fixture.compactor.counters().segments_rewritten, 1);
        assert!(
            elapsed >= Duration::from_millis(200),
            "the copy ran at device speed, {elapsed:?}",
        );
    }

    // a rotted sole copy is compacted once, not on every tick
    #[test]
    fn a_rotted_segment_stops_being_a_target() {
        let sole = ReelConfig {
            repair: RepairPath::None,
            ..settings()
        };
        let fixture = fixture(sole.clone());
        for byte in 1..=3u8 {
            put(&fixture, byte, vec![byte; 4096]);
        }
        seal(&fixture);
        // two of the three shadowed, so the segment ranks without any pass having run
        for byte in 2..=3u8 {
            put(&fixture, byte, vec![byte ^ 0xff; 4096]);
        }
        fixture.reel.flush().expect("flush");

        let mut image = fixture.sim.durable_image();
        flip_first_payload(&mut image, &seg_path(1));
        let reopened = reopen(image, sole);

        let (segment, _) = reopened
            .compactor
            .select_target(&reopened.reel, &reopened.index, 0.5, None)
            .expect("a first target");
        reopened
            .compactor
            .compact_segment(&reopened.reel, &reopened.index, segment)
            .expect("compact");

        let again = reopened
            .compactor
            .select_target(&reopened.reel, &reopened.index, 0.5, None);
        assert!(
            again.is_none(),
            "the rotted segment is offered again: {again:?}"
        );
        assert_eq!(
            reopened.compactor.counters().segments_pinned_by_rot,
            1,
            "the volume holds bytes it cannot reclaim and says nothing about it",
        );
    }

    // the pin lasts as long as the segment does not change
    #[test]
    fn a_pinned_segment_returns_once_something_in_it_dies() {
        let sole = ReelConfig {
            repair: RepairPath::None,
            ..settings()
        };
        let fixture = fixture(sole.clone());
        for byte in 1..=3u8 {
            put(&fixture, byte, vec![byte; 4096]);
        }
        seal(&fixture);
        for byte in 2..=3u8 {
            put(&fixture, byte, vec![byte ^ 0xff; 4096]);
        }
        fixture.reel.flush().expect("flush");

        let mut image = fixture.sim.durable_image();
        flip_first_payload(&mut image, &seg_path(1));
        let reopened = reopen(image, sole);

        let (segment, _) = reopened
            .compactor
            .select_target(&reopened.reel, &reopened.index, 0.5, None)
            .expect("a first target");
        reopened
            .compactor
            .compact_segment(&reopened.reel, &reopened.index, segment)
            .expect("compact");

        // the rotted key itself overwritten, so the segment holds nothing live at all
        put(&reopened, 1, vec![0x11; 4096]);
        reopened.reel.flush().expect("flush");

        assert_eq!(
            reopened
                .compactor
                .select_whole_dead(&reopened.reel, &reopened.index, None)
                .map(|(segment, _)| segment),
            Some(SegmentId(1)),
            "a segment with nothing live left stays pinned",
        );
    }

    // the drain takes segments it can unlink, never a rewrite for order
    #[test]
    fn the_drain_never_takes_a_clustering_rewrite() {
        let clustered = ReelConfig {
            rewrite_on_seal: true,
            ..settings()
        };
        let fixture = fixture(clustered);
        // descending keys, so the segment's offsets and its key order disagree
        for byte in (1..=4u8).rev() {
            put(&fixture, byte, vec![byte; 4096]);
        }
        seal(&fixture);
        fixture.reel.flush().expect("flush");

        assert!(
            fixture
                .compactor
                .select_target(&fixture.reel, &fixture.index, 1.0, None)
                .is_some(),
            "a clustered volume offers its unsorted segment to a ranked selection",
        );
        assert!(
            fixture
                .compactor
                .select_whole_dead(&fixture.reel, &fixture.index, None)
                .is_none(),
            "the drain took a segment with live records in it",
        );
    }

    // the memo answers a sealed segment's footer facts the way a fresh read does
    #[test]
    fn memoized_footer_facts_match_a_fresh_read() {
        // one byte of footer cache, so a second segment's footer gives up the first and
        // nothing but the memo can answer the whole set twice without a read
        let clustered = ReelConfig {
            rewrite_on_seal: true,
            footer_cache: ByteCount::from_bytes(1),
            ..settings()
        };
        let fixture = fixture(clustered);
        // descending keys, so this segment's key order and its offsets disagree
        for byte in (1..=4u8).rev() {
            put(&fixture, byte, vec![byte; 4096]);
        }
        seal(&fixture);
        // ascending keys, so this one is already a run
        for byte in 5..=8u8 {
            put(&fixture, byte, vec![byte; 4096]);
        }
        seal(&fixture);
        fixture.reel.flush().expect("flush");

        let shared = fixture.reel.shared();
        let sealed: Vec<SegmentId> = fixture
            .index
            .segments_snapshot()
            .into_iter()
            .map(|(segment, _)| segment)
            .filter(|segment| shared.footer_of(*segment).expect("footer").is_some())
            .collect();
        assert!(
            sealed.len() >= 2,
            "the volume sealed {} segments",
            sealed.len()
        );

        let mut memoized = Vec::new();
        for segment in &sealed {
            let facts = fixture
                .compactor
                .facts_of(shared, *segment)
                .expect("facts")
                .expect("sealed");
            let footer = shared.footer_of(*segment).expect("footer").expect("sealed");
            assert_eq!(
                facts,
                footer_facts(&footer),
                "segment {}'s memo disagrees with its own footer",
                segment.as_u32(),
            );
            memoized.push(facts);
        }
        assert!(
            memoized.iter().any(|facts| facts.is_out_of_order),
            "no segment read as out of order, so the memo was never asked the question",
        );
        assert!(
            memoized.iter().any(|facts| facts.is_sorted_run),
            "no segment read as a run"
        );

        let before = fixture.sim.read_count();
        for (segment, facts) in sealed.iter().zip(&memoized) {
            let held = fixture.compactor.facts_of(shared, *segment).expect("facts");
            assert_eq!(held.as_ref(), Some(facts));
        }
        assert_eq!(
            fixture.sim.read_count(),
            before,
            "a held answer still went to the device"
        );

        // what a retire takes out, a fresh read puts back the same
        for (segment, facts) in sealed.iter().zip(&memoized) {
            fixture.compactor.forget_facts(*segment);
            let derived = fixture.compactor.facts_of(shared, *segment).expect("facts");
            assert_eq!(derived.as_ref(), Some(facts));
        }
    }

    // the rate is what decides how much of the volume a sweep covers
    #[test]
    fn the_rate_decides_the_sweep() {
        let passes_at = |scrub_mbps: u64| {
            let fixture = fixture(ReelConfig {
                scrub_mbps,
                ..settings()
            });
            let payload = vec![0x5a; 4_000];
            for record in 0..500u64 {
                let mut bytes = [0u8; KEY_WIDTH];
                bytes[..8].copy_from_slice(&record.to_be_bytes());
                let key = RecordKey::from_bytes(RECORDS, &bytes).expect("key");
                let committed = fixture
                    .reel
                    .put(key.clone(), payload.clone(), 0, Commit::PerRecord)
                    .expect("put");
                fixture
                    .index
                    .insert(&key, committed.loc, committed.lsn, None)
                    .expect("insert");
            }
            seal(&fixture);
            fixture.reel.flush().expect("flush");

            let mut passes = 0;
            loop {
                std::thread::sleep(std::time::Duration::from_millis(2));
                fixture
                    .compactor
                    .scrub_pass(&fixture.reel, &fixture.index)
                    .expect("pass");
                passes += 1;
                if fixture.compactor.scrub_resume_point().is_none() && passes > 1 {
                    return passes;
                }
                assert!(passes < 10_000, "the sweep never reached the end");
            }
        };

        let slow = passes_at(1);
        let fast = passes_at(200);

        assert!(
            fast < slow,
            "a {fast} pass sweep at the fast rate against {slow} at the slow one"
        );
    }

    // a zero scrub rate leaves a corrupt record in place for a later read to catch
    #[test]
    fn scrub_disabled_skips() {
        let disabled = ReelConfig {
            scrub_mbps: 0,
            ..settings()
        };
        let fixture = fixture(disabled.clone());
        put(&fixture, 1, vec![0x11; 200]);
        put(&fixture, 2, vec![0x22; 200]);
        seal(&fixture);
        fixture.reel.flush().expect("flush");

        let mut image = fixture.sim.durable_image();
        flip_first_payload(&mut image, &seg_path(1));
        let reopened = reopen(image, disabled);

        let hits = reopened
            .compactor
            .scrub_pass(&reopened.reel, &reopened.index)
            .expect("scrub");

        assert_eq!(hits, 0);
        assert!(reopened.index.get(&key(1)).expect("read").is_some());
    }

    // a scrub pass runs alongside ingest without stalling the writers
    #[test]
    fn scrub_allows_ingest() {
        let (store, _sim) = engine_store(scrub_config());
        for byte in 1..=4u8 {
            store.put(&key(byte), &[byte; 3_000]).expect("seed");
        }
        store.flush().expect("flush");
        let store = Arc::new(store);

        let barrier = Arc::new(Barrier::new(2));
        let writer_store = Arc::clone(&store);
        let writer_gate = Arc::clone(&barrier);
        let writer = thread::spawn(move || {
            writer_gate.wait();
            for byte in 10..=30u8 {
                writer_store.put(&key(byte), &[byte; 2_000]).expect("put");
            }
        });

        barrier.wait();
        for _ in 0..8 {
            store.scrub_once().expect("scrub");
        }
        writer.join().expect("join");

        for byte in 10..=30u8 {
            assert!(store.contains(&key(byte)).expect("read"));
        }
    }

    // a scrub pass under way turns a second caller away and never blocks a probe
    #[test]
    fn a_running_sweep_turns_the_next_caller_away() {
        let script = crate::sync::rendezvous::script();
        script.hold("scrub/segment");

        let fixture = fixture(settings());
        put(&fixture, 1, vec![0x11; 200]);
        put(&fixture, 2, vec![0x22; 200]);
        seal(&fixture);
        fixture.reel.flush().expect("flush");

        thread::scope(|scope| {
            let pass = scope.spawn(|| {
                fixture
                    .compactor
                    .scrub_pass(&fixture.reel, &fixture.index)
                    .expect("scrub")
            });
            script.await_reached("scrub/segment", 1);

            assert_eq!(
                fixture
                    .compactor
                    .scrub_pass(&fixture.reel, &fixture.index)
                    .expect("second caller"),
                0,
                "a sweep under way turns the second caller away"
            );
            let _probe = fixture.compactor.scrub_resume_point();

            script.release("scrub/segment");
            pass.join().expect("join");
        });
    }

    // an out-of-space append is refused and the store stays consistent
    #[test]
    fn enospc_append_stays_consistent() {
        let config = ReelConfig {
            segment_bytes: ByteCount::mb(1),
            ..engine_config()
        };
        let plan = FaultPlan::new(1).with_fault(ENOSPC_AT, FaultKind::EnospcAppend);
        let store = crate::engine::ReelStore::open_with_io(
            PathBuf::from("/bulk"),
            config,
            COLUMNS,
            Arc::new(SimIo::new(plan)),
        )
        .expect("open");

        let mut rejected = Vec::new();
        for byte in 1..=8u8 {
            if store.put(&key(byte), &[byte; 2_000]).is_err() {
                rejected.push(byte);
            }
        }

        assert!(!rejected.is_empty());
        for byte in &rejected {
            assert!(!store.contains(&key(*byte)).expect("read"));
        }
        let mut recount = 0u64;
        for byte in 1..=8u8 {
            if store.contains(&key(byte)).expect("read") {
                recount += 1;
            }
        }
        assert_eq!(store.totals().count, recount);
    }

    // many compaction passes racing across threads keep every key and the totals
    #[test]
    fn concurrent_compaction_consistent() {
        let (store, _sim) = engine_store(engine_config());
        for byte in 1..=8u8 {
            store.put(&key(byte), &[byte; 3_000]).expect("put");
        }
        for byte in 1..=6u8 {
            store
                .put(&key(byte), &[byte + 50; 3_000])
                .expect("overwrite");
        }
        store.flush().expect("flush");
        let before = store.totals();
        let store = Arc::new(store);

        let mut passes = Vec::new();
        for _ in 0..4 {
            let store = Arc::clone(&store);
            passes.push(thread::spawn(move || {
                for _ in 0..3 {
                    store.compact_once().expect("compact");
                }
            }));
        }
        for pass in passes {
            pass.join().expect("join");
        }

        assert_eq!(store.totals().count, before.count);
        for byte in 1..=8u8 {
            assert!(store.contains(&key(byte)).expect("read"));
        }
    }
}
