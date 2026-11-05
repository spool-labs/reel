//! One volume: its roots, its open segments, its sealed segments and its sync state

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Mutex;

use reel_api::units::{ByteCount, RowCount, VolumeId, WritePlace};

use crate::config::SyncPolicy;
use crate::engine::open_segment::{OpenSegment, SegmentPhase};
use crate::engine::walk_job::WalkSlot;
use crate::guard::hold;
use crate::index::open_slots::{OpenSlot, OpenSlots};
use crate::io::Io;
use crate::pool::Pool;
use crate::segment::damage_list::DamageList;
use crate::segment::segment_table::{RetiredSegment, SegmentTable};
use crate::segment::segment_tail::SegmentNumber;
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
    pub retired: RetiredSegment<File>,
    pub segment: SegmentNumber,
    /// A failed save keeps the file, or open would find a row with no file
    pub is_saved: bool,
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
    pub segments_unlinked: AtomicU64,
    pub segments_parked: AtomicU64,
    pub parked: Mutex<[Option<ParkedSegment<Backend::File>>; MOST_PARKED]>,
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
