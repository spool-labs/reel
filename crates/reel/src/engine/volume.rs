//! One volume: its roots, its open segments, its sealed segments and its sync state

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Mutex;

use reel_api::units::{ByteCount, RowCount, VolumeId, WritePlace};

use crate::config::SyncPolicy;
use crate::engine::open_segment::{OpenSegment, SegmentPhase};
use crate::engine::ration::Ration;
use crate::engine::walk_job::WalkSlot;
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
    pub bytes_since_sync: ByteCount,
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
    /// Rewrites this lane has finished
    pub rewrites_done: AtomicU64,
    /// Outputs a failed remove left here. A lane with any starts nothing until they go
    pub strays: AtomicU64,
}

impl<File> RewriteLane<File> {
    /// Start a lane with nothing running
    pub fn new() -> Self {
        Self {
            is_rewriting: AtomicBool::new(false),
            output_floor: AtomicU64::new(NO_SEAL),
            merging: Mutex::new([SegmentNumber(0); MOST_MERGE_SEGMENTS]),
            inputs: Mutex::new([SegmentNumber(0); MOST_MERGE_SEGMENTS]),
            partial_outputs: Mutex::new(Default::default()),
            strays: AtomicU64::new(0),
            rewrites_done: AtomicU64::new(0),
        }
    }
}

impl<File> Default for RewriteLane<File> {
    fn default() -> Self {
        Self::new()
    }
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
    /// Holds compaction off. Only tests set this today
    pub is_compaction_held: AtomicBool,
    /// A seal found the table or a directory full, and only a reclaim can give it room
    pub is_seal_stuck: AtomicBool,
    /// The segment a seal is working on, or `NO_SEAL`. Its rows can still answer through its open slot, so no walk starts on it
    pub sealing: AtomicU64,
    pub walk_slots: Vec<WalkSlot<Backend::Task>>,
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
    pub rows_in_index: AtomicU64,
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
    /// The segment the walk has as its target, or `NO_SEAL`
    pub walk_segment: AtomicU64,
    /// A test holds the walk here to leave segments unwalked
    pub is_walk_held: AtomicBool,
    /// A test holds rebuilds off, to see what damage does before one runs
    pub is_rebuild_held: AtomicBool,
    /// A walk is counting what its covers hide, and a row that left now would make an older one look live
    pub is_applying: AtomicBool,
    /// Counts the turns a walk stood aside for a rewrite
    pub walks_held: AtomicU64,
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
    pub fn table_holds_any(&self) -> bool {
        self.highest_sealed.load(Ordering::Acquire) > 0
    }

    /// Check the volume still takes writes
    pub fn has_failed(&self) -> bool {
        self.is_failed.load(Ordering::Acquire)
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

    /// Check the park has room for this many segments
    pub fn can_park(&self, count: usize) -> bool {
        self.park_room() >= count
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
        hold(&self.parked).iter().filter(|slot| slot.is_none()).count()
    }

    /// Read the handover counter a get compares before it fills the spot cache
    pub fn handover_count(&self) -> u64 {
        self.handovers.load(Ordering::Acquire)
    }

    /// Count the open segments that take claims
    pub fn taking_count(&self) -> usize {
        let claims = hold(&self.claims);
        let mut count = 0;
        for segment in &claims.open {
            if segment.phase == SegmentPhase::Taking {
                count += 1;
            }
        }
        count
    }
}
