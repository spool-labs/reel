//! One volume: its roots, its open segments, its sealed segments and its sync state

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Mutex;

use reel_api::maintenance_report::{PICK_OUTCOMES, PickOutcome, SHARE_KINDS, SPAN_RUN_BUCKETS, SealSteps};
use reel_api::units::{ByteCount, Nanoseconds, RowCount, VolumeId, WritePlace};

use crate::config::SyncPolicy;
use crate::engine::open_segment::OpenSegment;
use crate::engine::ration::Ration;
use crate::engine::held_bits::HeldBits;
use crate::engine::walk_job::WalkSlot;
use crate::engine::row_job::IDLE_SEAL_AFTER;
use crate::guard::hold;
use crate::index::open_slots::{OpenSlot, OpenSlots};
use crate::io::Io;
use crate::pool::Pool;
use crate::segment::damage_list::DamageList;
use crate::segment::segment_table::{RetiredSegment, SegmentTable};
use crate::segment::segment_tail::{MOST_MERGE_SEGMENTS, SegmentNumber};
use crate::segment::range_covers::RangeCovers;
use crate::segment::summary::{LostRows, SummaryPaths, SummaryRows};
use crate::superblock::SuperblockPaths;
use crate::write_order::WriteOrder;

/// A volume's `sealing` number between seals
pub const NO_SEAL: u64 = u64::MAX;

/// A volume's room for segments out of every list whose files wait for their reads to finish
pub const MOST_PARKED: usize = 8;

/// A segment out of every list, whose file goes once no read holds it
#[derive(Clone, Debug)]
pub struct ParkedSegment<File> {
    /// Its slot and handle, which a file open couldn't remove never had
    pub retired: Option<RetiredSegment<File>>,
    pub segment: SegmentNumber,
    /// The summary that dropped this row is on disk, or the next look at the park saves it first
    pub is_saved: bool,
}

/// Most segments one merge writes, its grave output aside
pub const MOST_MERGE_OUTPUTS: usize = 8;
/// The slot of a merge's grave output, past its data outputs
pub const GRAVE_SLOT: usize = MOST_MERGE_OUTPUTS;
/// The outputs a merge has made so far, the last data output and the grave output among them
pub type OutputSlots<File> = [Option<RewriteOutput<File>>; MOST_MERGE_OUTPUTS + 1];

/// The file a rewrite is writing
#[derive(Clone, Debug)]
pub struct RewriteOutput<File> {
    pub file: File,
    pub root: usize,
    pub number: SegmentNumber,
    /// The tail is durable and the file has a segment's name
    pub is_named: bool,
}

/// Where a volume's sync stands
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SyncState {
    pub started: u64,
    pub completed: u64,
    pub is_running: bool,
}

/// The open segments, behind the short claim lock
#[derive(Debug)]
pub struct ClaimState<File> {
    pub open: Vec<OpenSegment<File>>,
    pub is_creating: bool,
    pub prepared: Option<OpenSegment<File>>,
    pub wants_prepared: bool,
    pub next_root: usize,
}

/// Segments a rewrite gave up on with nothing to win, which a later pick skips. The oldest leaves when the list fills, for the
/// cost of one more look
pub const MOST_GAVE_UP: usize = 64;

/// The segments that gave up since something last changed that could free a row or a grave
#[derive(Clone, Copy, Debug)]
pub struct GaveUp {
    segments: [SegmentNumber; MOST_GAVE_UP],
    next: usize,
    changes: u64,
}

impl GaveUp {
    /// Start with nothing listed
    pub fn new() -> Self {
        Self {
            segments: [SegmentNumber(0); MOST_GAVE_UP],
            next: 0,
            changes: 0,
        }
    }

    /// Empty the list when something has changed since it was filled
    pub fn catch_up(&mut self, changes: u64) {
        if self.changes != changes {
            *self = Self { changes, ..Self::new() };
        }
    }

    /// Add a segment, in place of the oldest when the list is full
    pub fn add(&mut self, segment: SegmentNumber) {
        self.segments[self.next] = segment;
        self.next = (self.next + 1) % MOST_GAVE_UP;
    }

    /// Check a segment is listed
    pub fn holds(&self, segment: SegmentNumber) -> bool {
        self.segments.contains(&segment)
    }
}

impl Default for GaveUp {
    fn default() -> Self {
        Self::new()
    }
}

/// Ingest periods past a volume's last seal start that it stays hot, one missed seal
pub const HOT_PERIODS: u64 = 2;

/// What one running rewrite shares with the rest of the volume
#[derive(Debug)]
pub struct RewriteLane<File> {
    pub is_rewriting: AtomicBool,
    /// The lowest segment this rewrite is writing, or `NO_SEAL`
    pub output_floor: AtomicU64,
    /// The inputs of a merge running beside the walk, which the walk leaves alone
    pub merging: Mutex<[SegmentNumber; MOST_MERGE_SEGMENTS]>,
    /// Every input of the rewrite running here
    pub inputs: Mutex<[SegmentNumber; MOST_MERGE_SEGMENTS]>,
    pub partial_outputs: Mutex<OutputSlots<File>>,
    /// The rows the walk found dead in this rewrite's inputs while it held them, as bits in the rewrite's region
    pub held: Mutex<HeldBits>,
    /// Whether the rewrite holds its inputs, so a charge on them goes to its bits
    pub is_holding: AtomicBool,
    /// Rewrites this lane has finished
    pub rewrites_done: AtomicU64,
    /// Outputs a failed remove left here. A lane with any starts nothing until they go
    pub strays: AtomicU64,
}

impl<File> RewriteLane<File> {
    /// Start a lane with nothing running
    pub fn new() -> Self {
        let lane = Self {
            is_rewriting: AtomicBool::new(false),
            output_floor: AtomicU64::new(NO_SEAL),
            merging: Mutex::new([SegmentNumber(0); MOST_MERGE_SEGMENTS]),
            inputs: Mutex::new([SegmentNumber(0); MOST_MERGE_SEGMENTS]),
            partial_outputs: Mutex::new(Default::default()),
            held: Mutex::new(HeldBits::default()),
            is_holding: AtomicBool::new(false),
            strays: AtomicU64::new(0),
            rewrites_done: AtomicU64::new(0),
        };
        // a mutex's first lock can allocate, and a pick that asks a lane after open allocates nothing
        drop(hold(&lane.merging));
        drop(hold(&lane.inputs));
        drop(hold(&lane.partial_outputs));
        drop(hold(&lane.held));
        lane
    }
}

impl<File> Default for RewriteLane<File> {
    fn default() -> Self {
        Self::new()
    }
}

/// File bytes and dead bytes of some segments
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RowBytes {
    pub file: u64,
    pub dead: u64,
}

/// What a test holds off. A build without the test hooks has none of these, and every check reads false
#[cfg(feature = "test-hooks")]
#[derive(Debug, Default)]
pub struct TestHolds {
    /// Compaction stops stepping
    pub compaction: AtomicBool,
    /// The walk takes no new target, so segments stay unwalked
    pub walk: AtomicBool,
    /// Rebuilds wait, to see what damage does before one runs
    pub rebuild: AtomicBool,
    /// Quiet-time reclaim waits, so a segment keeps the few dead rows it was built with
    pub reclaim: AtomicBool,
    /// The overlap bound stays at its least whatever the records' size, for tests of merge mechanics with large values
    pub small_bound: AtomicBool,
    /// A merge promises a quarter of its estimate, as counts that charged dead rows twice would
    pub low_estimate: AtomicBool,
    /// Walks look keys up through the table alone, to compare against the cursor
    pub no_cursor: AtomicBool,
}

/// One volume of a store
pub struct Volume<Backend: Io> {
    pub id: VolumeId,
    pub name: String,
    pub roots: Vec<PathBuf>,
    pub superblock_paths: Vec<SuperblockPaths>,
    pub policy: SyncPolicy,
    pub locks: Mutex<Vec<Backend::Lock>>,
    pub claims: Mutex<ClaimState<Backend::File>>,
    pub table: SegmentTable<Backend::File>,
    pub view: AtomicU64,
    pub handovers: AtomicU64,
    pub highest_sealed: AtomicU64,
    pub sync: Mutex<SyncState>,
    pub is_failed: AtomicBool,
    pub segment_bytes: ByteCount,
    pub segment_rows: RowCount,
    pub walk_retries: AtomicU64,
    pub walks_waiting: AtomicU64,
    pub wants_compaction: AtomicBool,
    pub has_kept_covers: AtomicBool,
    /// What tests hold off, in a build with the test hooks
    #[cfg(feature = "test-hooks")]
    pub holds: TestHolds,
    /// A seal found the table or a directory full, and only a reclaim can give it room
    pub is_seal_stuck: AtomicBool,
    /// Times a seal stopped on a full table or directory, once for each stop until a seal lands again
    pub seal_stops: AtomicU64,
    /// The bytes the distinct filesystems under the roots hold, or nothing when a root's disk has no known size
    pub disk_bytes: Option<ByteCount>,
    /// The disk bytes the table's entries cover, which a disk past them leaves to the seal stop
    pub disk_covered: ByteCount,
    /// The segment size the cover assumes: the byte target, or a full segment's rows at the row floor when that is less
    pub cover_segment_bytes: ByteCount,
    /// Bytes each table entry plans for, by the volume's columns
    pub entry_bytes: ByteCount,
    /// Looks for rewrite work, by what each came to, in the order of `PickOutcome`
    pub picks: [AtomicU64; PICK_OUTCOMES],
    /// File bytes of the inputs finished rewrites took off the disk
    pub rewrite_inputs_gone: AtomicU64,
    /// File bytes of finished rewrites' outputs
    pub rewrite_written: AtomicU64,
    /// Bytes the outputs took, by the kinds the report counts them in
    pub rewrite_written_by_share: [AtomicU64; SHARE_KINDS],
    /// Bytes past the records the outputs of each kind took
    pub rewrite_footers_by_share: [AtomicU64; SHARE_KINDS],
    pub rewrite_gone_by_share: [AtomicU64; SHARE_KINDS],
    /// Rewrites done, by the same kinds
    pub rewrites_by_share: [AtomicU64; SHARE_KINDS],
    /// File bytes whole unlinks took off the disk
    pub unlinked_bytes: AtomicU64,
    /// Dead bytes the inputs that left held
    pub rewrite_dead_gone: AtomicU64,
    /// Dead bytes the outputs carry
    pub rewrite_dead_kept: AtomicU64,
    /// Rewrites that wrote at least the bytes their leaving inputs held
    pub rewrites_unreclaimed: AtomicU64,
    /// Walk blocks whose counts landed
    pub walk_blocks: AtomicU64,
    /// The segment a seal is working on, or `NO_SEAL`. Its rows can still answer through its open slot, so no walk starts on it
    pub sealing: AtomicU64,
    pub walk_slots: Vec<WalkSlot>,
    /// Every root's filesystem can give a file's blocks back in place
    pub can_free_blocks: bool,
    pub bytes_freed: AtomicU64,
    /// A segment has large dead records no list could hold, for one pass to free once no cue is live
    pub has_held_frees: AtomicBool,
    pub seals_done: AtomicU64,
    pub segments_unlinked: AtomicU64,
    pub segments_parked: AtomicU64,
    /// Empty files a failed remove left with the park full, which the next open removes
    pub empties_left: AtomicU64,
    pub parked: Mutex<[Option<ParkedSegment<Backend::File>>; MOST_PARKED]>,
    pub segments_rewritten: AtomicU64,
    /// A rewrite has made its first row decision and not installed. Rows it decided on must stay as they were, so no walk
    /// starts a target
    /// A named output a failed rewrite could not remove is still on disk
    pub stray_outputs: AtomicU64,
    /// Counts the times a file that has to go would not, which a volume stuck that way shows here
    pub reclaim_retries: AtomicU64,
    /// The walk is part way through a segment
    pub has_walk_target: AtomicBool,
    pub gave_up: Mutex<GaveUp>,
    pub early_bar_rewrites: AtomicU64,
    pub plain_bar_rewrites: AtomicU64,
    /// Rewrites that copied in key order, reading records at random
    pub key_order_copies: AtomicU64,
    /// Rewrites that copied each input's records in offset order
    pub offset_order_copies: AtomicU64,
    /// Reads the offset-order copy took a run of records in
    pub span_reads: AtomicU64,
    /// Records those reads took
    pub span_records: AtomicU64,
    /// Bytes those reads took between their records
    pub span_gap_bytes: AtomicU64,
    /// Records the offset-order copy read alone
    pub lone_copies: AtomicU64,
    /// Records a merge read one at a time as their keys came, in key order or kept on the second look
    pub keyed_copies: AtomicU64,
    /// Row-block bytes merges read deciding, placing and copying
    pub footer_reads: [AtomicU64; 3],
    /// The offset-order copy's reads by the records each took, a bucket a power of two
    pub span_runs: [AtomicU64; SPAN_RUN_BUCKETS],
    /// Whole summary saves since open
    pub summary_saves: AtomicU64,
    pub order: WriteOrder,
    pub rows_in_index: AtomicU64,
    pub seal_steps: SealStepTimes,
    pub ration: Ration,
    pub marked_seals: AtomicU64,
    pub marked_seal_bytes: AtomicU64,
    pub marks_dropped: AtomicU64,
    pub is_spinning: bool,
    pub is_device_set: bool,
    /// One lane for each rewrite that can run at once: the planner's, then a worker's each
    pub lanes: Vec<RewriteLane<Backend::File>>,
    /// Rows the open segments reach before a seal starts, from the measured rates and set as each seal ends
    pub seal_point: AtomicU64,
    /// Rows a second the last seal ran at, or 0 before one has finished
    pub seal_rate: AtomicU64,
    /// Rows a second the volume took between the last two seals, or 0 before there are two
    pub ingest_rate: AtomicU64,
    /// When the last sealed segment last took a write, in the clock's nanoseconds, or 0 before a seal
    pub seal_started_at: AtomicU64,
    /// A writer crossed the cadence limit and the row job owes the volume a sync
    pub is_sync_due: AtomicBool,
    /// Bytes written since the last byte-cadence sync
    pub bytes_since_sync: AtomicU64,
    /// When the row job last saw a write land on the volume
    pub last_write_at: AtomicU64,
    /// Bytes merges wrote and gave up, by the phase they stopped in
    pub given_up_by_phase: [AtomicU64; 6],
    /// Bytes merges wrote and gave up, by why: interrupted, out of room, an input gone, bad data, anything else
    pub given_up_by_kind: [AtomicU64; 5],
    /// Park slots the running merges claimed for their inputs
    pub parks_promised: AtomicUsize,
    /// Installs that waited past MOST_PARK_WAIT for park room
    pub long_park_waits: AtomicU64,
    /// Merges that wrote past their promise and took more room
    pub promises_outgrown: AtomicU64,
    /// The disk sat under the writers' reserve at the last pick, so overlap merges wait
    pub is_room_short: AtomicBool,
    /// A merge was refused for disk room, so the planner looks again on a timer, since no write may come to wake it
    pub is_room_refused: AtomicBool,
    /// Bytes of outputs the running merges claimed room for
    pub outputs_promised: AtomicU64,
    /// Seals that ended a segment writes had gone quiet on
    pub idle_seals: AtomicU64,
    /// Nanoseconds from the last sealed segment's first row to its seal start, the time the writes took to fill it, or 0 before a
    /// seal
    pub seal_interval: AtomicU64,
    /// Nanoseconds the last overlap merge took for each input it merged, or 0 before one has run
    pub merge_interval: AtomicU64,
    /// A seal may have taken overlap past the bound at a point a merge can start, so record picks wait until the planner's
    /// look says otherwise
    pub is_over_bound: AtomicBool,
    /// The segment the walk has as its target, or `NO_SEAL`
    pub walk_segment: AtomicU64,
    /// A walk is counting what its covers hide, and a row that left now would make an older one look live
    pub is_applying: AtomicBool,
    /// Orders a walk's landing against a rewrite that begins or ends holding its inputs
    pub claim_lock: Mutex<()>,
    /// Moves each time a rewrite begins or ends holding, so a block counted before it counts again
    pub claims_changed: AtomicU64,
    /// Counts the turns a walk stood aside for a rewrite
    pub walks_held: AtomicU64,
    /// Counts the waits of a block whose charges land on an input a rewrite has claimed, none until the held bits come
    pub walk_claim_waits: AtomicU64,
    /// Nanoseconds those waits held the walk, in all
    pub walk_claim_held: AtomicU64,
    /// Nanoseconds the longest one held the walk
    pub walk_claim_longest: AtomicU64,
    /// Held blocks the drains read, each once
    pub held_drain_blocks: AtomicU64,
    /// The bytes those reads took
    pub held_drain_bytes: AtomicU64,
    pub summary: Mutex<SummaryRows>,
    pub summary_paths: SummaryPaths,
    pub lost_rows: Mutex<LostRows>,
    pub summary_buffer: Pool<Vec<u8>>,
    pub damage: DamageList,
    pub covers: RangeCovers,
    pub damaged: Mutex<Vec<SegmentNumber>>,
    pub is_clearing: AtomicBool,
    pub reserve: ByteCount,
}

impl<Backend: Io> Volume<Backend> {
    /// Check a test holds compaction off
    #[cfg(feature = "test-hooks")]
    pub fn is_compaction_held(&self) -> bool {
        self.holds.compaction.load(Ordering::Acquire)
    }

    /// Check a test holds compaction off, which a build without the hooks never does
    #[cfg(not(feature = "test-hooks"))]
    pub const fn is_compaction_held(&self) -> bool {
        false
    }

    /// Check a test holds the walk off
    #[cfg(feature = "test-hooks")]
    pub fn is_walk_held(&self) -> bool {
        self.holds.walk.load(Ordering::Acquire)
    }

    /// Check a test holds the walk off, which a build without the hooks never does
    #[cfg(not(feature = "test-hooks"))]
    pub const fn is_walk_held(&self) -> bool {
        false
    }

    /// Check a test holds rebuilds off
    #[cfg(feature = "test-hooks")]
    pub fn is_rebuild_held(&self) -> bool {
        self.holds.rebuild.load(Ordering::Acquire)
    }

    /// Check a test holds rebuilds off, which a build without the hooks never does
    #[cfg(not(feature = "test-hooks"))]
    pub const fn is_rebuild_held(&self) -> bool {
        false
    }

    /// Check a test holds the overlap bound at its least
    #[cfg(feature = "test-hooks")]
    pub fn is_bound_pinned(&self) -> bool {
        self.holds.small_bound.load(Ordering::Acquire)
    }

    /// Check a test holds the overlap bound at its least, which a build without the hooks never does
    #[cfg(not(feature = "test-hooks"))]
    pub const fn is_bound_pinned(&self) -> bool {
        false
    }
    /// Check a test has walks look keys up through the table alone
    #[cfg(feature = "test-hooks")]
    pub fn is_cursor_held_off(&self) -> bool {
        self.holds.no_cursor.load(Ordering::Acquire)
    }

    /// Check a test has walks skip the cursor, which a build without the hooks never does
    #[cfg(not(feature = "test-hooks"))]
    pub const fn is_cursor_held_off(&self) -> bool {
        false
    }

    /// Check a test cuts merge promises to a quarter of the estimate
    #[cfg(feature = "test-hooks")]
    pub fn is_estimate_low(&self) -> bool {
        self.holds.low_estimate.load(Ordering::Acquire)
    }

    /// Check a test cuts merge promises, which a build without the hooks never does
    #[cfg(not(feature = "test-hooks"))]
    pub const fn is_estimate_low(&self) -> bool {
        false
    }


    /// Check a test holds quiet-time reclaim off
    #[cfg(feature = "test-hooks")]
    pub fn is_reclaim_held(&self) -> bool {
        self.holds.reclaim.load(Ordering::Acquire)
    }

    /// Check a test holds quiet-time reclaim off, which a build without the hooks never does
    #[cfg(not(feature = "test-hooks"))]
    pub const fn is_reclaim_held(&self) -> bool {
        false
    }

    /// Read the set of open segments a reader counts, in one load
    pub fn view(&self) -> OpenSlots {
        OpenSlots(self.view.load(Ordering::Acquire))
    }

    /// Add a slot to the view
    pub fn open_slot(&self, slot: OpenSlot) {
        self.view.fetch_or(OpenSlots::only(slot).0, Ordering::AcqRel);
    }

    /// Switch a sealing segment to sealed, in one atomic store
    pub fn close_slot(&self, slot: OpenSlot) {
        self.view.fetch_and(!OpenSlots::only(slot).0, Ordering::AcqRel);
    }

    /// Check any sealed segment has reached the table
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn table_holds_any(&self) -> bool {
        self.highest_sealed.load(Ordering::Acquire) > 0
    }

    /// Check the volume still takes writes. A lock found poisoned anywhere fails every volume, since the state behind it may be
    /// half written
    pub fn has_failed(&self) -> bool {
        self.is_failed.load(Ordering::Acquire) || crate::guard::is_poisoned()
    }

    /// Note that a sync failed, so the volume refuses writes until the store is reopened
    pub fn fail(&self) {
        self.is_failed.store(true, Ordering::Release);
    }

    /// Note a sealed segment's highest place
    pub fn note_sealed(&self, place: WritePlace) {
        self.highest_sealed.fetch_max(place.0, Ordering::AcqRel);
    }

    /// Check a replayed row could sit under a newer row in a sealed segment
    pub fn may_hide_under_sealed(&self, place: WritePlace) -> bool {
        place.0 < self.highest_sealed.load(Ordering::Acquire)
    }

    /// Flag a segment for the pass that frees what no list could hold
    pub fn note_held_frees(&self, segment: SegmentNumber) {
        self.table.flag_held_frees(segment, true);
        self.has_held_frees.store(true, Ordering::Release);
    }

    /// Check the volume takes writes hot: its writes fill a segment faster than the last overlap merge took one, so merging
    /// would fall behind and only tax ingest, and its last seal started less than two fill times ago. A fill time runs from the
    /// sealed segment's first row to its seal start. A trickle that a merge outpaces, a volume no merge has run on yet, and one
    /// whose writes stopped are quiet
    pub fn is_hot(&self, now: Nanoseconds) -> bool {
        let fill = self.seal_interval.load(Ordering::Relaxed);
        let merge = self.merge_interval.load(Ordering::Relaxed);
        fill < merge && self.is_ingesting(now)
    }

    /// Check writes are still arriving: the last seal started less than two fill times ago. Record rewrites, which spend writes
    /// only to win space back, wait while it holds, and a volume with no seal yet or whose writes stopped is past it
    pub fn is_ingesting(&self, now: Nanoseconds) -> bool {
        now.0 < self.hot_until().0
    }

    /// Read when the hot window closes: two fill times past the last seal start, or a quiet spell past the last write
    pub fn hot_until(&self) -> Nanoseconds {
        let interval = self.seal_interval.load(Ordering::Relaxed);
        if interval == 0 {
            return Nanoseconds(0);
        }
        let filled = self.seal_started_at.load(Ordering::Relaxed).saturating_add(HOT_PERIODS.saturating_mul(interval));
        Nanoseconds(filled.min(self.last_write_at.load(Ordering::Relaxed).saturating_add(IDLE_SEAL_AFTER.0)))
    }

    /// Check the volume runs short of room: the fullest shard of its table or its park is past half, so merges run under hot
    /// ingest
    pub fn is_short_of_room(&self) -> bool {
        let parked = hold(&self.parked).iter().filter(|slot| slot.is_some()).count();
        2 * parked > MOST_PARKED || self.table.is_past_half()
    }

    /// Check the park has room for this many segments
    pub fn can_park(&self, count: usize) -> bool {
        self.park_room() >= count
    }

    /// Count what a finished rewrite took off the disk and wrote back
    pub fn note_rewrite_bytes(&self, gone: RowBytes, written: RowBytes, kind: usize) {
        self.rewrite_inputs_gone.fetch_add(gone.file, Ordering::Relaxed);
        self.rewrite_written.fetch_add(written.file, Ordering::Relaxed);
        self.rewrite_written_by_share[kind].fetch_add(written.file, Ordering::Relaxed);
        self.rewrite_gone_by_share[kind].fetch_add(gone.file, Ordering::Relaxed);
        self.rewrites_by_share[kind].fetch_add(1, Ordering::Relaxed);
        self.rewrite_dead_gone.fetch_add(gone.dead, Ordering::Relaxed);
        self.rewrite_dead_kept.fetch_add(written.dead, Ordering::Relaxed);
        if written.file >= gone.file {
            self.rewrites_unreclaimed.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Count what a look for rewrite work came to
    pub fn note_pick(&self, outcome: PickOutcome) {
        self.picks[outcome.index()].fetch_add(1, Ordering::Relaxed);
    }

    /// Check a merge running beside the walk has taken a segment
    pub fn is_merging(&self, segment: SegmentNumber) -> bool {
        self.lanes.iter().any(|lane| hold(&lane.merging).contains(&segment))
    }

    /// Check a running rewrite has taken a segment as an input, so no other lane picks it
    pub fn is_taken(&self, segment: SegmentNumber) -> bool {
        self.lanes.iter().any(|lane| hold(&lane.inputs).contains(&segment))
    }

    /// Check a rewrite may start: a spinning disk runs one stream at a time, so a second would only seek between them
    pub fn can_start_rewrite(&self) -> bool {
        !self.is_spinning || self.lanes.iter().all(|lane| hold(&lane.inputs)[0] == SegmentNumber(0))
    }

    /// Check a rewrite sits between its first row decision and its install
    pub fn is_rewriting(&self) -> bool {
        self.lanes.iter().any(|lane| lane.is_rewriting.load(Ordering::Acquire))
    }

    /// Read the lowest segment any rewrite is writing, or `NO_SEAL`. A slot at or above it can point at a segment the table has
    /// yet to take
    pub fn output_floor(&self) -> u64 {
        self.lanes.iter().map(|lane| lane.output_floor.load(Ordering::Acquire)).min().unwrap_or(NO_SEAL)
    }

    /// Check no read holds a parked segment any more
    pub fn is_unpinned(&self, parked: &ParkedSegment<Backend::File>) -> bool {
        parked.retired.as_ref().is_none_or(|retired| self.table.retired_pins(retired) == 0)
    }

    /// Count the park's free slots
    pub fn park_room(&self) -> usize {
        self.free_park_slots().saturating_sub(self.parks_promised.load(Ordering::Acquire))
    }

    /// Count the park's free slots, with none set aside for merges under way
    pub fn free_park_slots(&self) -> usize {
        hold(&self.parked).iter().filter(|slot| slot.is_none()).count()
    }

    /// Read the handover counter a get compares before it fills the spot cache
    pub fn handover_count(&self) -> u64 {
        self.handovers.load(Ordering::Acquire)
    }

}

/// The longest each step of a seal has run, for the report
#[derive(Debug, Default)]
pub struct SealStepTimes {
    pub drain: AtomicU64,
    pub walk_step: AtomicU64,
    pub footer: AtomicU64,
    pub install: AtomicU64,
    pub clean_step: AtomicU64,
}

impl SealStepTimes {
    /// Read the times for a report
    pub fn reading(&self) -> SealSteps {
        let read = |step: &AtomicU64| Nanoseconds(step.load(Ordering::Relaxed));
        SealSteps {
            drain: read(&self.drain),
            walk_step: read(&self.walk_step),
            footer: read(&self.footer),
            install: read(&self.install),
            clean_step: read(&self.clean_step),
        }
    }
}
