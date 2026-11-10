//! Everything a store shares between its threads

use std::io::{Error, ErrorKind};
use std::path::Path;
use std::mem;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock};

use reel_api::column_id::ColumnId;
use reel_api::maintenance_report::{PAUSE_CAUSES, PICK_KINDS, PauseCause, UnitKind, WRITE_SOURCES, WriteSource};
use reel_api::memory_report::MemoryReport;
use reel_api::store_error::StoreError;
use reel_api::units::{ByteCount, FileOffset, Nanoseconds, SegmentCount, WritePlace};

use crate::engine::rewrite_job::rows_of;
use crate::engine::admission::{Admission, Admit, Decision, JobRole, reserve_for, room_for};
use reel_api::maintenance_report::WorkerPriority;
use crate::engine::column_info::ColumnInfo;
use crate::engine::cue::{CueTable, JobCounts};
use crate::engine::engine_sizing::EngineSizing;
use crate::engine::job_tally::{JobTallies, UnitTallies};
use crate::engine::open_segment::{RecordWindow, RowQueue};
use crate::engine::compact_job::park;
use crate::engine::open_segment::{data_path, output_path, rows_path};
use crate::engine::read_pool::ReadPool;
use crate::engine::walk_job::{FREE_BLOCK_BYTES, FreeRecord};
#[cfg(any(test, feature = "test-hooks"))]
use crate::segment::summary_orders::WalkList;
use crate::engine::workers::Workers;
use crate::engine::volume::{ParkedSegment, RewriteOutput, Volume};
use crate::segment::range_covers::Cover;
use crate::guard::hold;
use crate::region::Region;
use crate::index::open_slots::OpenSlot;
use crate::index::spot_index::SpotIndex;
use crate::index::write_index::WriteIndex;
use crate::io::ring::{ReadRing, RingDriver};
use crate::io::{Io, IoFile, IoTask};
use crate::pool::Pool;
use crate::record::record_check::RecordKey;
use crate::record::record_encoder::RecordEncoder;
use crate::record::record_reader::RecordReader;
use crate::rows::row_group::RowGroupBuffer;
use crate::engine::complete_image::ImageFiles;
use crate::index::complete_table::CompleteTable;
use crate::index::record_cache::RecordCache;
use crate::segment::block_cache::BlockCache;
use crate::segment::filter_arena::FilterArena;
use crate::segment::row_block::ROW_BLOCK_BYTES;
use crate::segment::column_directory::ColumnDirectory;
use crate::segment::footer_reader::FooterReader;
use crate::segment::segment_tail::{MAX_COLUMNS, SegmentNumber};
use crate::segment::summary::write_summary;
use crate::superblock::Superblock;
use crate::write_order::PlaceCount;

/// Segment numbers one superblock write reserves
pub const SEGMENT_NUMBER_BATCH: u64 = 1_024;
/// Places one limit reserves: a raise costs a superblock sync per root once per 2^32 rows, about 70
/// minutes at a million rows a second, and each open skips what's left of a run, so 2^64 outlasts 2^32 opens
pub const PLACE_RUN: u64 = 1 << 32;

/// The segment in a slot and its data file
#[derive(Clone, Debug)]
pub struct SlotData<File> {
    pub segment: SegmentNumber,
    pub file: File,
}

/// One open slot's file, kept under a short lock
#[derive(Debug)]
pub struct SlotFile<File> {
    pub data: RwLock<Option<SlotData<File>>>,
    pub leaving_rows: Mutex<Option<File>>,
    pub pins: AtomicU32,
    pub volume: AtomicUsize,
}

/// The state every store handle and job shares
pub struct EngineState<Backend: Io> {
    pub io: Backend,
    pub record_key: RecordKey,
    pub sizing: EngineSizing,
    pub columns: Vec<Option<ColumnInfo>>,
    pub directories: Vec<Option<ColumnDirectory>>,
    pub volumes: Vec<Volume<Backend>>,
    pub places: Arc<PlaceCount>,
    pub index: WriteIndex,
    pub spot: SpotIndex,
    pub filter_cache: BlockCache,
    pub filter_arena: FilterArena,
    pub row_cache: BlockCache,
    pub record_cache: RecordCache,
    pub queues: Vec<Mutex<RowQueue>>,
    pub spare_groups: Vec<Mutex<Option<RowGroupBuffer>>>,
    pub windows: Vec<[RecordWindow; 2]>,
    pub window_copies: Vec<Mutex<Vec<u8>>>,
    pub slot_files: Vec<SlotFile<Backend::File>>,
    pub free_slots: Mutex<Vec<OpenSlot>>,
    pub encoders: Pool<RecordEncoder>,
    pub stored_shares: Pool<Vec<u8>>,
    pub batch_rows: Pool<RowGroupBuffer>,
    pub batch_records: Pool<Vec<u8>>,
    pub batch_lengths: Pool<Vec<u32>>,
    pub current_mark: AtomicU64,
    pub longest_turn: AtomicU64,
    pub index_waits: AtomicU64,
    pub longest_index_wait: AtomicU64,
    pub values_under_least: Vec<AtomicU64>,
    pub open_time: AtomicU64,
    pub prefault_time: AtomicU64,
    pub record_readers: Pool<RecordReader>,
    pub footer_readers: Pool<FooterReader>,
    pub job_footer_readers: Pool<FooterReader>,
    pub scan_heap: ByteCount,
    pub superblock: Mutex<Superblock>,
    pub superblock_bytes: Mutex<Vec<u8>>,
    pub next_segment: AtomicU64,
    pub reserved_segment: AtomicU64,
    pub is_reserving: AtomicBool,
    pub deletes: AtomicU64,
    pub row_job_idle: AtomicBool,
    pub damaged_segments: AtomicU64,
    pub dropped_summary_rows: AtomicU64,
    pub tallies: JobTallies,
    pub written: [AtomicU64; WRITE_SOURCES.len()],
    /// Pauses writers took, by what they waited for
    pub pauses: [AtomicU64; PAUSE_CAUSES.len()],
    /// The longest room step, in nanoseconds
    pub longest_room_step: AtomicU64,
    /// Walk lookups through the cursor, in the report's order
    pub cursor_lookups: [AtomicU64; 4],
    /// Merge lookups, in the report's order
    pub merge_lookups: [AtomicU64; 6],
    /// Walk block lookups, in the report's order
    pub block_lookups: [AtomicU64; 4],
    pub units: UnitTallies,
    pub opened_at: Nanoseconds,
    pub graves_at_open: AtomicU64,
    pub graves_dropped: AtomicU64,
    pub graves_carried: AtomicU64,
    pub empty_rewrites: AtomicU64,
    pub corruption_errors: AtomicU64,
    pub is_arena_full: AtomicBool,
    pub arena_fill: AtomicU64,
    pub unlinks_refused: AtomicU64,
    pub memory_budget: ByteCount,
    pub seal_heap: AtomicU64,
    pub walk_heap: AtomicU64,
    pub worker_priority: Mutex<WorkerPriority>,
    pub pick_holds: [AtomicU64; PICK_KINDS.len()],
    pub workers: Workers<Backend::Task>,
    pub compaction_heap: AtomicU64,
    pub admission: Admission,
    pub job_count: AtomicU64,
    pub row_task: OnceLock<Backend::Task>,
    pub reserve_task: OnceLock<Backend::Task>,
    pub read_tasks: Vec<OnceLock<Backend::Task>>,
    pub read_pool: ReadPool,
    pub ring: Option<ReadRing<Backend::Ring>>,
    pub ring_task: OnceLock<Backend::Task>,
    pub ring_record_readers: Pool<RecordReader>,
    pub ring_footer_readers: Pool<FooterReader>,
    pub is_read_only: bool,
    pub complete: Vec<Option<CompleteTable>>,
    pub complete_images: Vec<Option<Mutex<ImageFiles>>>,
    pub complete_cache: Option<BlockCache>,
    pub complete_line_bytes: ByteCount,
    pub seal_tasks: Vec<OnceLock<Backend::Task>>,
    pub planner_task: OnceLock<Backend::Task>,
    pub index_waiters: AtomicU64,
    pub cues: CueTable,
    pub is_stopping: AtomicBool,
}

/// A held open-slot pin, released when dropped
#[derive(Debug)]
pub struct SlotPin<'state, File> {
    pub file: File,
    slot: &'state SlotFile<File>,
}

impl<File> Drop for SlotPin<'_, File> {
    fn drop(&mut self) {
        self.slot.pins.fetch_sub(1, Ordering::AcqRel);
    }
}

impl<File: Clone> SlotFile<File> {
    /// Start an empty slot
    pub fn new() -> Self {
        let slot = Self {
            data: RwLock::new(None),
            leaving_rows: Mutex::new(None),
            pins: AtomicU32::new(0),
            volume: AtomicUsize::new(0),
        };
        // some platforms set a lock up at its first use, and that's a job for open
        drop(hold(&slot.leaving_rows));
        slot
    }

    /// Read the file the slot holds now
    pub fn file(&self) -> Option<File> {
        let guard = match self.data.read() {
            Ok(guard) => guard,
            Err(poisoned) => crate::guard::recover(poisoned),
        };
        guard.as_ref().map(|held| held.file.clone())
    }

    /// Hold a segment's file for a read, or return nothing when the slot has moved on from it
    pub fn pin(&self, segment: SegmentNumber) -> Option<SlotPin<'_, File>> {
        let guard = match self.data.read() {
            Ok(guard) => guard,
            Err(poisoned) => crate::guard::recover(poisoned),
        };
        let held = guard.as_ref()?;
        if held.segment != segment {
            return None;
        }
        let file = held.file.clone();
        self.pins.fetch_add(1, Ordering::AcqRel);
        Some(SlotPin { file, slot: self })
    }

    /// Set the slot's file
    pub fn set(&self, file: Option<SlotData<File>>) {
        let mut guard = match self.data.write() {
            Ok(guard) => guard,
            Err(poisoned) => crate::guard::recover(poisoned),
        };
        *guard = file;
    }

    /// Count the reads still holding the slot
    pub fn pin_count(&self) -> u32 {
        self.pins.load(Ordering::Acquire)
    }
}

impl<File: Clone> Default for SlotFile<File> {
    fn default() -> Self {
        Self::new()
    }
}

impl<Backend: Io> EngineState<Backend> {
    /// Find a declared column
    pub fn column(&self, column: ColumnId) -> Result<ColumnInfo, StoreError> {
        match self.columns[column.0 as usize] {
            Some(info) => Ok(info),
            None => Err(StoreError::UnknownColumn { column }),
        }
    }

    /// Read how big a column's row blocks can get, with a column no store declares taking the usual size
    pub fn block_bytes_of(&self, column: ColumnId) -> usize {
        self.columns[column.0 as usize].map_or(ROW_BLOCK_BYTES, |info| info.block_bytes())
    }

    /// Read the write mark a new read starts from
    pub fn read_point(&self, volume: &Volume<Backend>) -> WritePlace {
        volume.order.mark()
    }

    /// Count a summary row that did not fit
    pub fn note_dropped_row(&self) {
        self.dropped_summary_rows.fetch_add(1, Ordering::AcqRel);
    }

    /// Count summary rows given up for rows that must stay
    pub fn note_dropped_rows(&self, rows: u64) {
        self.dropped_summary_rows.fetch_add(rows, Ordering::AcqRel);
    }

    /// Write a volume's summary whole from a copy, so no lock is held over the writes, and one save runs at a time
    pub fn save_summary(&self, volume: &Volume<Backend>) -> Result<(), Error> {
        volume.summary_saves.fetch_add(1, Ordering::Relaxed);
        volume.summary_buffer.with(|buffer| {
            let started = self.io.now();
            buffer.clear();
            let rows = hold(&volume.summary);
            buffer.extend_from_slice(rows.bytes());
            let forward = rows.highest_forward;
            drop(rows);
            let bytes = buffer.len() as u64;
            self.note_unit(UnitKind::SummaryCopy, bytes, started);
            let saved = write_summary(&self.io, &volume.summary_paths, self.read_point(volume), forward, buffer);
            self.note_unit(UnitKind::SummarySave, bytes, started);
            self.note_written(WriteSource::Summaries, ByteCount(bytes));
            saved
        })
    }

    /// Check the saved limit or segment counter is more than half used
    pub fn needs_reservation(&self) -> bool {
        let next_segment = self.next_segment.load(Ordering::Acquire);
        let reserved = self.reserved_segment.load(Ordering::Acquire);
        self.places.left() < PLACE_RUN / 2 || reserved.saturating_sub(next_segment) < SEGMENT_NUMBER_BATCH / 2
    }

    /// Save the next limit and segment counter to every live root, and publish them once a root holds them,
    /// so no place goes out above what a root saved: a root that can't take them fails its volume alone, no
    /// lock is held over the writes, and one save runs at a time. Return false when no root took them
    pub fn reserve_ahead(&self) -> bool {
        if self.is_reserving.swap(true, Ordering::AcqRel) {
            return true;
        }

        let mut bytes = mem::take(&mut *hold(&self.superblock_bytes));
        let (limit, reserved) = {
            let mut superblock = hold(&self.superblock);
            let next_segment = self.next_segment.load(Ordering::Acquire).max(superblock.next_segment.0);
            superblock.place_limit = WritePlace(superblock.place_limit.0 + PLACE_RUN);
            superblock.next_segment = SegmentNumber(next_segment + SEGMENT_NUMBER_BATCH);
            superblock.revision += 1;
            superblock.write_bytes(&mut bytes);
            (superblock.place_limit, superblock.next_segment)
        };

        let mut is_held = false;
        for volume in &self.volumes {
            for paths in &volume.superblock_paths {
                if volume.has_failed() {
                    continue;
                }
                match Superblock::write_file(&self.io, paths, &bytes) {
                    Ok(()) => is_held = true,
                    Err(_failed) => volume.fail(),
                }
            }
        }

        *hold(&self.superblock_bytes) = bytes;
        if is_held {
            self.places.set_limit(limit);
            self.reserved_segment.store(reserved.0, Ordering::Release);
        }
        self.is_reserving.store(false, Ordering::Release);
        is_held
    }

    /// Take the next segment number, and only save more here when the job has fallen a whole batch behind
    pub fn next_segment_number(&self) -> Result<SegmentNumber, StoreError> {
        let number = self.next_segment.fetch_add(1, Ordering::AcqRel);
        while number >= self.reserved_segment.load(Ordering::Acquire) {
            if !self.reserve_ahead() {
                return Err(StoreError::Io {
                    action: "saving a superblock",
                    source: Error::other("no root took the next reservation"),
                });
            }
            if number >= self.reserved_segment.load(Ordering::Acquire) {
                self.pause_for(PauseCause::Reservation);
            }
        }
        Ok(SegmentNumber(number))
    }

    /// Take a free open slot for a volume
    pub fn take_slot(&self, volume: usize) -> Option<OpenSlot> {
        let slot = hold(&self.free_slots).pop()?;
        self.slot_files[slot.0 as usize].volume.store(volume, Ordering::Release);
        Some(slot)
    }

    /// Check a slot is free
    pub fn has_free_slot(&self) -> bool {
        !hold(&self.free_slots).is_empty()
    }

    /// Give a slot back once nothing uses it
    pub fn free_slot(&self, slot: OpenSlot) {
        hold(&self.free_slots).push(slot);
    }

    /// Count a volume's segments waiting for the dead-byte walk
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn pending_walks(&self, volume: usize) -> usize {
        let rows = hold(&self.volumes[volume].summary);
        rows.orders().walk_count(WalkList::Unwalked) + rows.orders().walk_count(WalkList::Backward)
    }

    /// Count the times a volume's walk has had to try a block again
    pub fn walk_retries(&self, volume: usize) -> u64 {
        self.volumes[volume].walk_retries.load(Ordering::Relaxed)
    }

    /// Check a volume's summary lookup still holds every row
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn is_summary_lookup_whole(&self, volume: usize) -> bool {
        hold(&self.volumes[volume].summary).is_lookup_whole()
    }

    /// Remove a data file that's in no list and say so, or hold it in the park for a retry. A full park leaves it for the next
    /// open, and only an empty file may come this far with one, so the count of those is the report's
    pub fn remove_or_park(&self, volume: &Volume<Backend>, root: &Path, number: SegmentNumber) -> bool {
        let is_gone = match self.io.remove_file(&data_path(root, number)) {
            Ok(()) => true,
            Err(error) => error.kind() == ErrorKind::NotFound,
        };
        // the directory has to know before the rows go: a power cut could keep their removal and lose this one, and a data file
        // with no rows and no tail reads as damage
        let is_gone = is_gone && self.io.sync_directory(root).is_ok();
        if !is_gone {
            if !volume.can_park(1) {
                volume.empties_left.fetch_add(1, Ordering::Relaxed);
            }
            park(volume, None, number, true);
        }
        is_gone
    }

    /// Remove a parked segment's file and close it, after saving the summary if the save that dropped its row failed. Any
    /// failure hands the segment back to stay parked, since no grave may drop while a file that could bring its key back is
    /// on disk
    pub fn clear_parked(
        &self,
        volume: &Volume<Backend>,
        parked: ParkedSegment<Backend::File>,
    ) -> Result<(), ParkedSegment<Backend::File>> {
        // the file goes only behind a saved summary, or open would find a row with no file
        if !parked.is_saved && self.save_summary(volume).is_err() {
            volume.reclaim_retries.fetch_add(1, Ordering::Relaxed);
            return Err(parked);
        }
        for root in &volume.roots {
            // a retry finds the file gone, and its removal still has to reach the directory
            match self.io.remove_file(&data_path(root, parked.segment)) {
                Ok(()) => {},
                Err(error) if error.kind() == ErrorKind::NotFound => {},
                Err(_error) => {
                    volume.reclaim_retries.fetch_add(1, Ordering::Relaxed);
                    return Err(parked);
                },
            }
            // a file open held can still have its rows beside it, and the rows go only once the data file's going is on disk
            if self.io.sync_directory(root).is_err() {
                volume.reclaim_retries.fetch_add(1, Ordering::Relaxed);
                return Err(parked);
            }
            match self.io.remove_file(&rows_path(root, parked.segment)) {
                Ok(()) => {},
                Err(error) if error.kind() == ErrorKind::NotFound => {},
                Err(_error) => {
                    volume.reclaim_retries.fetch_add(1, Ordering::Relaxed);
                    return Err(parked);
                },
            }
            if self.io.sync_directory(root).is_err() {
                volume.reclaim_retries.fetch_add(1, Ordering::Relaxed);
                return Err(parked);
            }
        }
        volume.segments_parked.fetch_sub(1, Ordering::AcqRel);
        if let Some(retired) = parked.retired {
            volume.table.free_retired(&retired);
            self.io.close_file(retired.file);
        }
        Ok(())
    }

    /// Remove a rewrite's output under whichever name it reached and close it. A named output that could still come back at a
    /// crash is handed back, since its rows would answer again from under any grave dropped since
    pub fn remove_output(
        &self,
        volume: &Volume<Backend>,
        output: RewriteOutput<Backend::File>,
    ) -> Result<(), RewriteOutput<Backend::File>> {
        let root = &volume.roots[output.root];
        let path = if output.is_named { data_path(root, output.number) } else { output_path(root, output.number) };
        let removed = self.io.remove_file(&path);
        // a file with no segment's name goes at the next open whatever happens here
        if !output.is_named {
            let _synced = self.io.sync_directory(root);
            self.io.close_file(output.file);
            return Ok(());
        }
        match removed {
            Ok(()) => {},
            Err(error) if error.kind() == ErrorKind::NotFound => {},
            Err(_error) => {
                volume.reclaim_retries.fetch_add(1, Ordering::Relaxed);
                return Err(output);
            },
        }
        if self.io.sync_directory(root).is_err() {
            volume.reclaim_retries.fetch_add(1, Ordering::Relaxed);
            return Err(output);
        }
        self.io.close_file(output.file);
        Ok(())
    }

    /// Give a dead record's whole blocks back, or say it has to wait for a live cue or an unpublished replacement. The count
    /// moves first, so a read at an older point that fails its record check starts over, and one that takes its point after
    /// has the replacement under it. A filesystem that refuses leaves the record to a rewrite
    pub fn free_record(&self, volume: &Volume<Backend>, free: &FreeRecord) -> bool {
        if !volume.can_free_blocks {
            return true;
        }
        if self.read_point(volume) < free.replacement {
            return false;
        }
        self.cues.note_free();
        if self.cues.has_cue_between(volume.id.0 as usize, free.place, free.replacement) {
            return false;
        }
        let start = free.offset.0.next_multiple_of(FREE_BLOCK_BYTES);
        let end = (free.offset.0 + free.length.0) / FREE_BLOCK_BYTES * FREE_BLOCK_BYTES;
        if let (Some(pin), true) = (volume.table.pin(free.segment), start < end) {
            let length = ByteCount(end - start);
            let held = pin.file.held_bytes();
            if pin.file.free_range(FileOffset(start), length).is_ok()
                && let (Ok(before), Ok(after)) = (held, pin.file.held_bytes())
            {
                volume.bytes_freed.fetch_add(before.0.saturating_sub(after.0), Ordering::AcqRel);
            }
        }
        true
    }
    /// Count the segments with filters the arena lacks, and their columns. Gets read those filters through the block cache
    pub fn arena_left_out(&self) -> (SegmentCount, usize) {
        let (mut segments, mut columns) = (0, 0);
        for volume in &self.volumes {
            let rows = hold(&volume.summary);
            for row in rows.rows().filter(|row| !row.facts.is_damaged && !self.filter_arena.has_filters(row.facts.segment)) {
                let searched = (0..MAX_COLUMNS).map(|id| ColumnId(id as u8)).filter(|column| self.complete_table(*column).is_none());
                let left = searched.filter(|column| volume.table.column_blocks(row.facts.segment, *column).is_some()).count();
                segments += u64::from(left > 0);
                columns += left;
            }
        }
        (SegmentCount(segments), columns)
    }

    /// Add up the rows of the segments whose filters the arena holds
    pub fn arena_rows(&self) -> u64 {
        let mut total = 0;
        for volume in &self.volumes {
            let rows = hold(&volume.summary);
            for row in rows.rows().filter(|row| self.filter_arena.has_filters(row.facts.segment)) {
                total += rows_of(&row);
            }
        }
        total
    }

    /// Note whether filters wait for the arena, and keep how long the first fill took after open
    pub fn note_arena(&self, has_filters_left: bool) {
        self.is_arena_full.store(!has_filters_left, Ordering::Release);
        if !has_filters_left && self.arena_fill.load(Ordering::Relaxed) == 0 {
            let took = self.io.now().0.saturating_sub(self.opened_at.0);
            self.arena_fill.store(took.max(1), Ordering::Relaxed);
        }
    }

    /// Map a maintenance job's region when its need fits the host's free memory past the reserve, or when no region is held,
    /// and count a wait when it doesn't
    pub fn admit(&self, role: JobRole, need: ByteCount, may_floor: bool) -> Option<Region> {
        let admit = self.decide_admit(role, need, may_floor)?;
        self.map_admitted(&admit)
    }

    /// Decide a job's need against the host's free memory past the reserve, or let it in when no region is held, and count
    /// it held with no mapping yet, so the job maps on its own thread. A refused start counts a wait
    pub fn decide_admit(&self, role: JobRole, need: ByteCount, may_floor: bool) -> Option<Admit> {
        let memory = self.io.host_memory();
        let footers = ByteCount(self.volumes.iter().map(|volume| hold(&volume.summary).footer_bytes().0).sum());
        let reserve = reserve_for(memory.reserve_total(), footers);
        self.admission.note_reading(memory.free(), reserve);
        let decision = self.admission.decide(need, room_for(&memory, reserve));
        // a need the floor may not take waits, and only a start with no smaller need to try counts the wait
        let is_admitted = matches!(decision, Decision::Admit) || (decision == Decision::Floor && may_floor);
        if !is_admitted {
            if may_floor {
                self.admission.waits.fetch_add(1, Ordering::Relaxed);
            }
            return None;
        }
        self.admission.take(role, need, decision == Decision::Floor);
        Some(Admit { role, need })
    }

    /// Map an admitted need, its pages faulted in here, or give the admission back and count a wait when the map fails
    pub fn map_admitted(&self, admit: &Admit) -> Option<Region> {
        let Ok(region) = Region::map(admit.need) else {
            self.admission.give_back(admit.role, admit.need, ByteCount(0));
            self.admission.waits.fetch_add(1, Ordering::Relaxed);
            return None;
        };
        self.admission.mapped_total.fetch_add(region.mapped_bytes().0, Ordering::Relaxed);
        Some(region)
    }

    /// Note a job gave its region back at the need it was admitted with, and the bytes its lists wrote
    pub fn give_back(&self, role: JobRole, need: ByteCount, touched: ByteCount) {
        self.admission.give_back(role, need, touched);
    }

    /// Bring the planner back to every volume's compaction
    pub fn ask_compaction(&self) {
        for volume in &self.volumes {
            volume.wants_compaction.store(true, Ordering::Release);
        }
        if let Some(task) = self.planner_task.get() {
            task.wake();
        }
    }

    /// Keep a slot only if no job started since the counts were read, and give it back if one did
    pub fn pin_holds(&self, slot: usize, counts: JobCounts) -> bool {
        let is_held = self.cues.counts() == counts;
        if !is_held {
            self.cues.release(slot);
        }
        is_held
    }

    /// Drop the covers a published one holds inside it, unless a live cue reads below it and still needs them
    pub fn drop_covers_inside(&self, volume: &Volume<Backend>, cover: &Cover) {
        if !volume.covers.has_inside(cover) {
            return;
        }
        self.cues.note_reclaim();
        if !self.cues.has_cue_below(volume.id.0 as usize, cover.place) {
            volume.covers.drop_inside(cover);
        } else {
            // the cue's going looks again, or this cover would stay past its need
            volume.has_kept_covers.store(true, Ordering::Release);
        }
    }

    /// Report the heap each structure holds
    pub fn memory_report(&self) -> MemoryReport {
        let (arena_segments_left_out, arena_columns_left_out) = self.arena_left_out();
        let (arena_used, arena_room) = self.filter_arena.used_bytes();
        let mut report = MemoryReport {
            budget: self.memory_budget,
            write_index: self.index.heap_bytes(),
            spot_cache: self.spot.heap_bytes(),
            record_cache: self.record_cache.heap_bytes(),
            dead_bits: ByteCount(self.volumes.iter().map(|volume| volume.dead_bits.heap_bytes().0).sum()),
            complete_tables: ByteCount(self.complete.iter().flatten().map(|table| table.heap_bytes().0).sum()),
            block_cache: ByteCount(self.filter_arena.heap_bytes().0 + self.filter_cache.heap_bytes().0 + self.row_cache.heap_bytes().0 + self.complete_cache_bytes()),
            footer_space: ByteCount(self.seal_heap.load(Ordering::Acquire) + self.walk_heap.load(Ordering::Acquire)),
            compaction: ByteCount(self.compaction_heap.load(Ordering::Acquire)),
            thread_stacks: ByteCount(self.job_count.load(Ordering::Acquire) * self.io.job_stack_bytes().0),
            open_slots: self.queues.len(),
            maintenance_workers: self.sizing.workers,
            least_budget: self.sizing.least_budget,
            room: self.sizing.room,
            open_time: Nanoseconds(self.open_time.load(Ordering::Acquire)),
            prefault_time: Nanoseconds(self.prefault_time.load(Ordering::Acquire)),
            is_arena_full: self.is_arena_full.load(Ordering::Acquire),
            arena_segments_left_out,
            arena_columns_left_out,
            arena_used,
            arena_room,
            arena_rows: self.arena_rows(),
            entry_line: self.sizing.entry_line,
            disk_bytes: ByteCount(self.volumes.iter().filter_map(|volume| volume.disk_bytes).map(|bytes| bytes.0).sum()),
            disk_covered: ByteCount(self.volumes.iter().map(|volume| volume.disk_covered.0).sum()),
            ..MemoryReport::default()
        };
        for (index, directory) in self.directories.iter().enumerate().filter_map(|(index, directory)| Some((index, directory.as_ref()?))) {
            report.block_indexes = ByteCount(report.block_indexes.0 + directory.heap_bytes().0);
            // a volume's columns share one spare, which counts once
            let is_first = !self.directories[..index].iter().flatten().any(|earlier| earlier.shares_spare(directory));
            if is_first {
                report.block_indexes = ByteCount(report.block_indexes.0 + directory.spare_bytes().0);
            }
        }
        for volume in &self.volumes {
            report.segment_table = ByteCount(report.segment_table.0 + volume.table.heap_bytes().0);
            report.damaged_spans = ByteCount(report.damaged_spans.0 + volume.damage.heap_bytes().0);
            report.range_deletes = ByteCount(report.range_deletes.0 + volume.covers.heap_bytes().0);
            let buffer = volume.summary_buffer.heap_bytes(|buffer| ByteCount(buffer.capacity() as u64));
            report.summary = ByteCount(report.summary.0 + hold(&volume.summary).heap_bytes().0 + buffer.0);
        }
        let readers = self.record_readers.heap_bytes(RecordReader::heap_bytes);
        let footers = self.footer_readers.heap_bytes(FooterReader::heap_bytes);
        report.read_buffers = ByteCount(readers.0 + footers.0 + self.job_footer_readers.heap_bytes(FooterReader::heap_bytes).0);
        report.scans = self.scan_heap;
        // the tables and their block lines come on top of the budget
        report.budget = ByteCount(report.budget.0 + report.complete_tables.0 + self.complete_line_bytes.0);
        let ring_readers = self.ring_record_readers.heap_bytes(RecordReader::heap_bytes).0 + self.ring_footer_readers.heap_bytes(FooterReader::heap_bytes).0;
        let ring = self.ring.as_ref().map_or(0, |ring| ring.heap_bytes().0 + ring.driver.heap_bytes().0);
        report.read_buffers = ByteCount(report.read_buffers.0 + self.read_pool.heap_bytes().0 + ring + ring_readers);
        let encoders = self.encoders.heap_bytes(RecordEncoder::heap_bytes);
        let shares = self.stored_shares.heap_bytes(|share| ByteCount(share.capacity() as u64));
        report.compress_buffers = ByteCount(encoders.0 + shares.0);
        let batch_rows = self.batch_rows.heap_bytes(RowGroupBuffer::heap_bytes);
        let batch_lengths = self.batch_lengths.heap_bytes(|lengths| ByteCount((lengths.capacity() * size_of::<u32>()) as u64));
        let batch_records = self.batch_records.heap_bytes(|records| ByteCount(records.capacity() as u64));
        report.queues = ByteCount(report.queues.0 + batch_rows.0 + batch_records.0 + batch_lengths.0);
        for queue in &self.queues {
            let queue = hold(queue);
            report.queues = ByteCount(report.queues.0 + queue.filling.heap_bytes().0);
        }
        for windows in &self.windows {
            report.queues = ByteCount(report.queues.0 + windows[0].heap_bytes().0 + windows[1].heap_bytes().0);
        }
        for copy in &self.window_copies {
            report.queues = ByteCount(report.queues.0 + hold(copy).capacity() as u64);
        }
        for group in &self.spare_groups {
            if let Some(group) = hold(group).as_ref() {
                report.queues = ByteCount(report.queues.0 + group.heap_bytes().0);
            }
        }
        report
    }

    /// Add up the heap the shared parts hold
    pub fn heap_bytes(&self) -> ByteCount {
        self.memory_report().heap_total()
    }
}


/// Keeps the planner aside while a writer waits for index room
#[derive(Debug)]
pub struct IndexWait<'state> {
    waiters: &'state AtomicU64,
}

impl<'state> IndexWait<'state> {
    /// Count one more writer waiting
    pub fn new(waiters: &'state AtomicU64) -> Self {
        waiters.fetch_add(1, Ordering::AcqRel);
        Self { waiters }
    }
}

impl Drop for IndexWait<'_> {
    fn drop(&mut self) {
        self.waiters.fetch_sub(1, Ordering::AcqRel);
    }
}

impl<Backend: Io> EngineState<Backend> {
    /// Move the count and every volume's mark past a place an open found, so no volume starts below a row it holds
    pub fn resume_past(&self, place: WritePlace) {
        self.places.resume_past(place);
        for volume in &self.volumes {
            volume.order.resume_past(place);
        }
    }
}
