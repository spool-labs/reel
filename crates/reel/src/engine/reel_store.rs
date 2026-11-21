//! A store: open, the calls consumers make, and close

use std::io::{Error, ErrorKind};
use std::mem;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use reel_core::column_id::ColumnId;
use reel_core::direction::Direction;
use reel_core::maintenance_report::MaintenanceReport;
use reel_core::memory_report::MemoryReport;
use reel_core::reads::ValueWindow;
use reel_core::store_error::StoreError;
use reel_core::value_list::ValueList;
use reel_core::units::{ByteCount, DeathMark, Nanoseconds, RowCount, VolumeId, WritePlace};
use reel_core::usage::{ColumnUsage, UsageReport};
use reel_core::write_batch::WriteBatch;

use crate::config::{ColumnConfig, ColumnIndex, DeviceClass, KeyShape, LEAST_SEGMENT_BYTES, MOST_SET_SEGMENT_BYTES, StoreConfig, VolumeConfig};
use crate::engine::dead_bits::{CHUNK_WORDS, DEAD_BITS_NAME, DeadBits, NEW_DEAD_BITS_NAME, SAVE_HEADS_PAGE};
use crate::engine::rewrite_job::{record_totals, rows_of};
use crate::segment::block_filter::BITS_PER_ROW;
use crate::engine::caller_activity::CallerCounts;
use crate::io::wake_line::WakeLine;
use crate::engine::admission::Admission;
use reel_core::maintenance_report::WorkerPriority;
use crate::engine::column_info::ColumnInfo;
use crate::engine::engine_sizing::{
    check_seal_blocks, complete_block_rows, complete_row_limit, index_headroom, rows_within_blocks, seal_blocks, set_segment_rows, split_idle_room,
    EngineSizing, FILTER_CACHE_QUARTERS, FILTER_SLOT_BYTES, GROUP_BYTES, LEAST_LZ4_BUDGET, LEAST_SHARD_ENTRIES, PLANNED_LINE_BYTES,
    SHARED_JOBS, StoreShape, GATHER_BYTES, assumed_segment_bytes, capped_entries, disk_entries, entry_cost, entry_cover, floor_entries,
    dead_bits_bytes, segment_bytes_for, target_rows, IndexAsk, index_ask, index_cap, is_index_exact, share_indexes,
};
use crate::engine::engine_state::{EngineState, PLACE_RUN, SEGMENT_NUMBER_BATCH, SlotFile};
use crate::engine::open_segment::{RecordWindow, RowQueue, SegmentPhase, fits_a_segment_path, reserve_step, set_reserve_step, window_bytes_for};
use crate::engine::ration::Ration;
use crate::engine::ring_job::RingReaper;
use crate::engine::read_only::{close_read_only, load_read_only};
use crate::engine::read_path::RecordWant;
use crate::engine::read_pool::{ReadPool, ReadWorker};
use crate::engine::recovery::{
    OpenMode,
    RootScan, SummaryFound, list_roots, load_covers, read_summaries, recover_segments, save_cleared_spans, scan_roots,
    pass_open_rows,
    settle_summaries,
};
use crate::engine::reserve_job::ReserveJob;
use crate::engine::row_job::{EnterWatch, RowJob};
use crate::engine::scan_state::{ScanRange, ScanState, from_key, in_range, under_prefix};
use crate::engine::seal_job::{MOST_SEGMENT_BLOCKS, SealJob};
use crate::engine::store_scan::StoreScan;
use crate::engine::compact_job::{CompactJob, park};
use crate::engine::cue::{Cue, CueTable};
use crate::engine::filter_job::FilterLoader;
use crate::engine::planner::Planner;
use crate::engine::scrub_job::ScrubJob;
use crate::engine::walk_job::{BlockCounter, WalkJob, WalkSlot, WalkWorker};
use crate::engine::workers::Workers;
use crate::engine::load_state::{LoadCell, PACE_PER_MILLE, PaceCell, SPACE_TARGET_PER_MILLE};
use crate::segment::summary_orders::row_dead_bytes;
use crate::engine::volume::{SealStepTimes, ClaimState, GaveUp, LookPass, MOST_MERGE_OUTPUTS, MOST_PARKED, NO_SEAL, RewriteLane, SyncState, Volume, BitsSave};
#[cfg(feature = "test-hooks")]
use crate::engine::volume::TestHolds;
use crate::guard::hold;
use crate::index::open_slots::OpenSlot;
use crate::engine::complete_image::ImageFiles;
use crate::index::complete_table::CompleteTable;
use crate::index::complete_table::planned_table_bytes;
use crate::index::sorted_keys::{MOST_KEY_BYTES, SortedKeys, planned_index_bytes, rows_for_bytes};
use crate::index::value_pages::{VALUE_PAGE_BYTES, ValuePages};
use crate::index::spot_index::SpotIndex;
use crate::index::write_index::WriteIndex;
use crate::io::lock_outcome::LockOutcome;
use crate::io::{Io, IoFile, IoTask};
use crate::pool::{Pool, touched_buffer, touched_capacity, touched_filled, touched_room, touched_zeroes};
use crate::record::codec::Codec;
use crate::record::record_check::RecordKey;
use crate::record::record_encoder::{RecordEncoder, SHARE_BYTES};
use crate::record::record_reader::RecordReader;
use crate::rows::row_group::RowGroupBuffer;
use crate::index::record_cache::{MOST_CACHED_BYTES, RecordCache};
use crate::segment::block_cache::{BlockCache, CACHE_SHARDS, CACHE_WAYS};
use crate::segment::filter_arena::FilterArena;
use crate::segment::segment_table::NO_SHARD;
use crate::segment::column_directory::{ColumnDirectory, DirectoryList, empty_list};
use crate::segment::damage_list::{DamageList, MOST_DAMAGED_SPANS, SPAN_KEY_BYTES};
use crate::segment::range_covers::RangeCovers;
use crate::segment::summary::{MOST_ROW_COLUMN_BYTES, MOST_ROW_FIXED_BYTES, MOST_ROW_SPAN_FIXED_BYTES};
use crate::segment::footer_reader::FooterReader;
use crate::segment::block_entry::BlockEntry;
use crate::segment::row_block::{COMPLETE_BLOCK_BYTES, ROW_BLOCK_BYTES};
use crate::segment::segment_table::{SegmentTable, TABLE_SHARDS, TableSizing};
use crate::segment::segment_tail::{MAX_COLUMNS, SegmentNumber};
use crate::superblock::{RootCopies, StoreId, StoredColumn, StoredVolume, Superblock, SuperblockPaths, stored_column};
use crate::write_order::{PlaceCount, WriteOrder};

/// The lock file in each root
pub const LOCK_NAME: &str = "lock";
/// The longest a finish sleeps between looks at its seals, so a wake it missed costs this much
pub const FINISH_WAIT: Duration = Duration::from_millis(10);

/// An open store
pub struct ReelStore<Backend: Io> {
    pub state: Arc<EngineState<Backend>>,
    is_closed: AtomicBool,
    config: StoreConfig,
}

/// Turn a backend failure at open into a store error
pub fn open_error(action: &'static str) -> impl Fn(Error) -> StoreError {
    move |source| StoreError::Io { action, source }
}

impl<Backend: Io> ReelStore<Backend> {
    /// Open a store: lock every root, read every tail, recover every open segment, start the jobs
    pub fn open(io: Backend, config: &StoreConfig) -> Result<Self, StoreError> {
        let opened_from = io.now();
        check_config(config)?;
        let locks = lock_roots(&io, config)?;
        let superblock = settle_superblock(&io, config)?;

        let disks = volume_disks(&io, config).map_err(open_error("reading a disk's size"))?;
        let ring = if ring_fits(config, &disks) { io.open_ring().map_err(open_error("opening a ring"))? } else { None };
        let sizing = store_sizing(config, ring.is_some(), &disks)?;

        // every root is listed before any tail is read, and the table is sized from what they hold
        let mut roots = Vec::with_capacity(config.volumes.len());
        for (index, volume) in config.volumes.iter().enumerate() {
            roots.push((index, volume.roots.clone()));
        }
        let listings = list_roots(&io, &roots).map_err(open_error("listing a root"))?;
        let record_key = superblock.record_key;
        let mut scan = scan_roots(&io, record_key, &listings, OpenMode::Writable).map_err(open_error("reading a tail"))?;
        let summaries = match read_summaries(&io, &roots) {
            Ok(summaries) => summaries,
            Err(error) => {
                scan.close_files(&io);
                return Err(open_error("reading a summary")(error));
            },
        };

        // the pools are built and touched here and as the jobs start, and open reports that time apart
        let touched_from = io.now();
        // the sorted indexes take their room from the caches before anything is built, by the keys they held at open
        let (sizing, sorted_sizes) = size_sorted_indexes(config, &sizing, &summaries);
        let mut state = build_state(OpenInputs { io, config, sizing: &sizing, disks: &disks, superblock, locks, scan: &scan, summaries });
        state.complete = complete_tables(config);
        state.sorted = sorted_indexes(config, &sorted_sizes);
        state.sorted_sizes = sorted_sizes;
        state.complete_images = complete_images(config);
        state.attach_ring(ring);
        state.prefault_time.store(state.io.now().0.saturating_sub(touched_from.0), Ordering::Release);
        for volume in &state.volumes {
            check_seal_blocks(volume.segment_rows, volume.seal_blocks, config.widest_key(volume.id.0 as usize)).map_err(open_error("checking a seal's block list"))?;
        }
        let state = Arc::new(state);
        if let Err(error) = bring_up(&state, &mut scan) {
            // a caller can retry, so a failed open leaves no handle behind
            let _stopped = state.stop_jobs();
            scan.close_files(&state.io);
            for volume in &state.volumes {
                state.close_volume(volume);
            }
            return Err(error);
        }
        state.open_time.store(state.io.now().0.saturating_sub(opened_from.0), Ordering::Release);
        state.huge_pages.store(crate::pool::huge_page_bytes().map_or(0, |bytes| bytes.0), Ordering::Release);

        Ok(Self {
            state,
            is_closed: AtomicBool::new(false),
            config: config.clone(),
        })
    }

    /// Count the summary rows that didn't fit, each one a segment whose damage would cover its columns whole
    pub fn dropped_summary_rows(&self) -> u64 {
        self.state.dropped_summary_rows.load(Ordering::Acquire)
    }

    /// Estimate the live bytes of the whole store, without a scan
    pub fn live_data_size_bytes(&self) -> ByteCount {
        self.state.live_data_size()
    }

    /// Estimate the keys of the whole store, without a scan
    pub fn key_count_estimate(&self) -> RowCount {
        self.state.key_count()
    }

    /// Fill a report with live and on-disk bytes of one column
    pub fn column_disk_usage(&self, column: ColumnId, report: &mut ColumnUsage) -> Result<(), StoreError> {
        self.check_open()?;
        self.state.column_usage(column, report)
    }

    /// Fill a report with used and free bytes of every volume and root
    pub fn volume_usage(&self, report: &mut UsageReport) -> Result<(), StoreError> {
        self.check_open()?;
        self.state.volume_usage(report)
    }

    /// Fill a report with each job's state, the reclaim counts and the volume readings
    pub fn maintenance_report(&self, report: &mut MaintenanceReport) {
        self.state.maintenance(report);
    }

    /// Report the heap each structure holds
    pub fn memory_report(&self) -> MemoryReport {
        self.state.memory_report()
    }

    /// Count the segments open kept and couldn't read
    pub fn damaged_segments(&self) -> u64 {
        self.state.damaged_segments.load(Ordering::Acquire)
    }

    /// Read the least budget a store of this config opens on as a writer, the one a refusal gives, with no store opened: its
    /// parts with the workers the config sets, or none, and no ring
    pub fn least_budget(io: &Backend, config: &StoreConfig) -> Result<ByteCount, StoreError> {
        least_budget_of(io, config, false)
    }

    /// Read the least budget a read-only open of this config takes, with no store opened
    pub fn least_read_only_budget(io: &Backend, config: &StoreConfig) -> Result<ByteCount, StoreError> {
        least_budget_of(io, config, true)
    }

    /// Open a store beside its writer, with no lock and no write. It sees the store as it stood, every batch whole or absent,
    /// until `refresh` looks again
    pub fn open_read_only(io: Backend, config: &StoreConfig) -> Result<Self, StoreError> {
        check_config(config)?;
        Ok(Self {
            state: load_read_only(&io, config)?,
            is_closed: AtomicBool::new(false),
            config: config.clone(),
        })
    }

    /// Take in what the writer has done since this view was taken. The old view goes first, so the heap never holds two, and
    /// a refresh that fails can be asked for again
    pub fn refresh(&mut self) -> Result<(), StoreError> {
        self.check_open()?;
        if !self.state.is_read_only {
            return Ok(());
        }
        close_read_only(&self.state);
        self.state = load_read_only(&self.state.io, &self.config)?;
        Ok(())
    }

    /// Count a write, which tells maintenance callers are still busy, and refuse it on a store that only looks
    pub fn check_writes(&self) -> Result<(), StoreError> {
        self.state.caller_ops.note();
        self.check_open()?;
        if self.state.is_read_only {
            return Err(StoreError::ReadOnly);
        }
        Ok(())
    }

    /// Count a read, which tells maintenance callers are still busy, and check the store is open
    pub fn check_read(&self) -> Result<(), StoreError> {
        self.state.caller_ops.note();
        self.check_open()
    }

    /// Refuse a call after close
    pub fn check_open(&self) -> Result<(), StoreError> {
        if self.is_closed.load(Ordering::Acquire) {
            return Err(StoreError::Closed);
        }
        Ok(())
    }

    /// Write a value, visible to every later read when this returns
    pub fn put(&self, column: ColumnId, key: &[u8], value: &[u8]) -> Result<(), StoreError> {
        self.check_writes()?;
        self.state.write_value(column, key, value, None)
    }

    /// Write a value that dies at a mark, so it sits with records that die near it
    pub fn put_marked(&self, column: ColumnId, key: &[u8], value: &[u8], mark: DeathMark) -> Result<(), StoreError> {
        self.check_writes()?;
        self.state.write_value(column, key, value, Some(mark))
    }

    /// Write a value that dies at a mark and take its buffer
    pub fn put_owned_marked(&self, column: ColumnId, key: &[u8], value: Vec<u8>, mark: DeathMark) -> Result<(), StoreError> {
        self.put_marked(column, key, &value, mark)
    }

    /// Tell the store the current mark, which seals every open group whose ceiling it has reached
    pub fn set_current_mark(&self, mark: DeathMark) {
        self.state.set_current_mark(mark);
    }

    /// Count the waits for room in the write index, and read the longest
    pub fn index_waits(&self) -> (u64, Nanoseconds) {
        let waits = self.state.index_waits.load(Ordering::Relaxed);
        (waits, Nanoseconds(self.state.longest_index_wait.load(Ordering::Relaxed)))
    }

    /// Read the longest a long batch has held a writing turn
    pub fn longest_turn(&self) -> Nanoseconds {
        Nanoseconds(self.state.longest_turn.load(Ordering::Relaxed))
    }

    /// Apply a batch whole, in batch order, visible to every later read when this returns
    pub fn write_batch(&self, batch: &WriteBatch) -> Result<(), StoreError> {
        self.check_writes()?;
        self.state.write_batch(batch)
    }

    /// Write a value and take its buffer, which a none column sends to the file as it stands
    pub fn put_owned(&self, column: ColumnId, key: &[u8], value: Vec<u8>) -> Result<(), StoreError> {
        self.put(column, key, &value)
    }

    /// Remove every key in [start, end) written before this call, as one record whatever the keys it covers
    pub fn delete_range(&self, column: ColumnId, start: &[u8], end: &[u8]) -> Result<(), StoreError> {
        self.check_writes()?;
        self.state.write_range_delete(column, start, end)
    }

    /// Write a grave
    pub fn delete(&self, column: ColumnId, key: &[u8]) -> Result<(), StoreError> {
        self.check_writes()?;
        self.state.write_grave(column, key)
    }

    /// Fill a buffer with the newest value, and say whether the key has one
    pub fn get(&self, column: ColumnId, key: &[u8], buf: &mut Vec<u8>) -> Result<bool, StoreError> {
        self.check_read()?;
        self.state.counted(self.state.get_latest(column, key, RecordWant::Whole, buf))
    }

    /// Answer every key at one point in the write order, with a bad record as that key's answer
    pub fn get_many(&self, column: ColumnId, keys: &[&[u8]], values: &mut ValueList) -> Result<(), StoreError> {
        self.check_read()?;
        let answer = self.state.get_many_now(column, keys, values);
        self.state.corruption_errors.fetch_add(values.corrupt_count(), Ordering::Relaxed);
        answer
    }

    /// Fill a buffer with a checked window of the value, and say whether the key has one
    pub fn get_range(
        &self,
        column: ColumnId,
        key: &[u8],
        window: ValueWindow,
        buf: &mut Vec<u8>,
    ) -> Result<bool, StoreError> {
        self.check_read()?;
        let want = RecordWant::Window(window);
        self.state.counted(self.state.get_latest(column, key, want, buf))
    }

    /// Take a read view at this point in the write order, which copies no data and never blocks a writer
    pub fn cue(&self) -> Result<Cue<Backend>, StoreError> {
        self.check_read()?;
        Cue::new(Arc::clone(&self.state))
    }

    /// Walk a range built with the scan_state helpers, which can say how many rows the caller takes
    pub fn iter(&self, range: &ScanRange) -> Result<StoreScan<Backend>, StoreError> {
        self.check_read()?;
        StoreScan::new(Arc::clone(&self.state), range)
    }

    /// Walk rows and keys from a key, which comes first when it has a row
    pub fn iter_from(&self, column: ColumnId, start: &[u8], direction: Direction) -> Result<StoreScan<Backend>, StoreError> {
        self.check_read()?;
        StoreScan::new(Arc::clone(&self.state), &from_key(column, start, direction))
    }

    /// Walk rows and keys in [start, end)
    pub fn iter_range(
        &self,
        column: ColumnId,
        start: &[u8],
        end: &[u8],
        direction: Direction,
    ) -> Result<StoreScan<Backend>, StoreError> {
        self.check_open()?;
        StoreScan::new(Arc::clone(&self.state), &in_range(column, start, end, direction))
    }

    /// Walk rows and keys under a prefix
    pub fn iter_prefix(&self, column: ColumnId, prefix: &[u8]) -> Result<StoreScan<Backend>, StoreError> {
        self.check_read()?;
        StoreScan::new(Arc::clone(&self.state), &under_prefix(column, prefix))
    }

    /// Count the keys under a prefix present now, reading no values
    pub fn count_prefix(&self, column: ColumnId, prefix: &[u8]) -> Result<RowCount, StoreError> {
        self.cue()?.count_prefix(column, prefix)
    }

    /// Add up the bytes the values under a prefix take on disk now, compressed where the column compresses, reading no values
    pub fn bytes_prefix(&self, column: ColumnId, prefix: &[u8]) -> Result<ByteCount, StoreError> {
        self.cue()?.bytes_prefix(column, prefix)
    }

    /// Check whether the key has a value, without reading the value
    pub fn contains(&self, column: ColumnId, key: &[u8]) -> Result<bool, StoreError> {
        self.check_read()?;
        self.state.counted(self.state.contains_now(column, key))
    }

    /// Clear a damaged span once the caller has repaired it from peers, up to an end key or the column's end
    pub fn clear_damage(&self, column: ColumnId, start: &[u8], end: Option<&[u8]>) -> Result<(), StoreError> {
        self.check_writes()?;
        let info = self.state.column(column)?;
        let volume = &self.state.volumes[info.volume.0 as usize];
        while volume.is_clearing.swap(true, Ordering::AcqRel) {
            self.state.io.pause();
        }
        let cleared = self.clear_spans(volume, column, start, end);
        volume.is_clearing.store(false, Ordering::Release);
        cleared
    }

    /// Clear spans in memory and in the summary, and go back to how they were when the save fails, so an
    /// error always means nothing changed
    pub(crate) fn clear_spans(
        &self,
        volume: &Volume<Backend>,
        column: ColumnId,
        start: &[u8],
        end: Option<&[u8]>,
    ) -> Result<(), StoreError> {
        volume.damage.keep_spare();
        if volume.damage.clear(column, start, end).is_err() {
            return Err(StoreError::DamagePoolFull { volume: volume.id });
        }
        save_cleared_spans(&self.state, volume);
        if let Err(source) = self.state.save_summary(volume) {
            volume.damage.restore_spare();
            save_cleared_spans(&self.state, volume);
            return Err(open_error("saving a summary")(source));
        }
        Ok(())
    }

    /// Make every write that returned before this call durable on every volume
    pub fn flush(&self) -> Result<(), StoreError> {
        // a flush on a timer is no call, so it leaves the store free to read quiet
        self.check_open()?;
        if self.state.is_read_only {
            return Err(StoreError::ReadOnly);
        }
        for volume in &self.state.volumes {
            self.state.sync_volume(volume)?;
        }
        Ok(())
    }

    /// Make every write to one volume that returned before this call durable, so a caller can order two volumes' batches on disk
    pub fn flush_volume(&self, volume: VolumeId) -> Result<(), StoreError> {
        self.check_open()?;
        if self.state.is_read_only {
            return Err(StoreError::ReadOnly);
        }
        let Some(state) = self.state.volumes.get(volume.0 as usize) else {
            return Err(StoreError::UnknownVolume { volume });
        };
        self.state.sync_volume(state)
    }

    /// Set every volume's pace: the copy bytes busy callers' rewrites under half dead may make for each thousand dead bytes the
    /// walk counts. Zero leaves them for quiet time
    pub fn set_pace_per_mille(&self, per_mille: u64) {
        for volume in &self.state.volumes {
            volume.pace_per_mille.store(per_mille, Ordering::Relaxed);
        }
    }

    /// Set the overlap bound for every volume in the process, for a bench that measures a store with no overlap merges. Zero
    /// gives the bound back to the records
    #[cfg(feature = "test-hooks")]
    pub fn set_bound_base(&self, bound: usize) {
        crate::engine::rewrite_job::BOUND_BASE.store(bound, Ordering::Relaxed);
        self.state.ask_compaction();
    }

    /// Set every volume's space target: the dead bytes a rewrite must win back, as thousandths of the live bytes, before busy
    /// callers' rewrites under half dead start
    pub fn set_space_target_per_mille(&self, per_mille: u64) {
        for volume in &self.state.volumes {
            volume.space_target_per_mille.store(per_mille, Ordering::Relaxed);
        }
    }

    /// Mark each volume's fullest taking segment to seal, as an idle volume does on its own
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn seal_now(&self) {
        for volume in &self.state.volumes {
            self.state.seal_fullest(volume);
        }
    }

    /// Seal every open segment, wait for the seals, and close. A finished store holds sealed segments alone: no open segment, no
    /// rows file, no room reserved past a segment's end and no spare made ahead, so a copy of its files opens anywhere, read-only
    /// as well. Its complete tables' images are saved as close saves them, and a read-only open reads them and removes none.
    /// The caller stops its writers first, since a write that lands after the last look leaves a segment open. A seal that waits
    /// for room on a full disk returns the error and leaves the store open, so a finish after room comes back completes it
    pub fn finish(&self) -> Result<(), StoreError> {
        self.check_open()?;
        if self.state.is_read_only {
            return Err(StoreError::ReadOnly);
        }
        let waits_before: Vec<u64> = self.state.volumes.iter().map(|volume| volume.seal_full_waits.load(Ordering::Acquire)).collect();
        loop {
            let mut is_any_open = false;
            for (volume, waits) in self.state.volumes.iter().zip(&waits_before).filter(|(volume, _waits)| !volume.has_failed()) {
                if volume.seal_full_waits.load(Ordering::Acquire) > *waits {
                    return Err(StoreError::Io {
                        action: "finishing a store",
                        source: Error::from(ErrorKind::StorageFull),
                    });
                }
                let seen = volume.progress.seen();
                let mut claims = hold(&volume.claims);
                for index in 0..claims.open.len() {
                    if claims.open[index].phase == SegmentPhase::Taking {
                        self.state.seal_at(&mut claims, volume, index);
                    }
                }
                let is_open = !claims.open.is_empty();
                drop(claims);
                if is_open {
                    is_any_open = true;
                    volume.progress.wait(seen, FINISH_WAIT);
                }
            }
            if !is_any_open {
                break;
            }
        }
        self.close()
    }

    /// Stop every job, make every returned write durable and release the locks
    pub fn close(&self) -> Result<(), StoreError> {
        if self.is_closed.swap(true, Ordering::AcqRel) {
            return Ok(());
        }

        if self.state.is_read_only {
            close_read_only(&self.state);
            return Ok(());
        }
        self.state.stop_jobs().map_err(open_error("stopping a job"))?;
        // a scan's heads skip the write index while a volume counts no open row, so a count under the rows its open segments hold
        // would hide rows. The jobs have stopped, and a seal that stopped part way keeps the count above the sum, never under
        #[cfg(feature = "test-hooks")]
        for volume in &self.state.volumes {
            let open: u64 = hold(&volume.claims).open.iter().map(|open| open.row_count.0).sum();
            assert!(volume.rows_in_index.load(Ordering::Acquire) >= open, "a volume counts fewer open rows than its open segments hold");
        }

        #[cfg(feature = "test-hooks")]
        if self.state.io.proves_dead_bits() {
            check_dead_counts(&self.state);
            check_dead_bits_true(&self.state);
        }

        let mut outcome = Ok(());
        for volume in &self.state.volumes {
            if !volume.has_failed() {
                outcome = outcome.and(self.state.sync_volume(volume));
            }
            if !volume.has_failed() {
                outcome = outcome.and(self.state.save_summary(volume).map_err(open_error("saving a summary")));
            }
            // the bits that changed since the last save go with the summary, or the next open drops every segment they touched. A
            // failed save costs the walk's work and no more
            if !volume.has_failed() && volume.bits_saved_at.load(Ordering::Acquire) != 0 && volume.dead_bits.is_changed.load(Ordering::Acquire) {
                let _saved = self.state.save_dead_bits(volume);
            }
            // writes that returned on a failed volume may not be durable, and the caller hears it
            if volume.has_failed() {
                outcome = outcome.and(Err(StoreError::VolumeFailed { volume: volume.id }));
            }
            // an image only saves the next open time, so one that fails costs that and no more
            if !volume.has_failed() {
                let _saved = self.state.save_images(volume.id.0 as usize);
            }
            outcome = outcome.and(self.state.drop_prepared(volume).map_err(open_error("removing a spare segment")));
            self.state.close_volume(volume);
            hold(&volume.locks).clear();
        }
        outcome
    }
}

impl<Backend: Io> Drop for ReelStore<Backend> {
    fn drop(&mut self) {
        // a drop has no caller to tell, and a clean close has already stopped the jobs
        let _stopped = self.state.stop_jobs();
    }
}

impl<Backend: Io> EngineState<Backend> {
    /// Stop every job
    pub(crate) fn stop_jobs(&self) -> Result<(), Error> {
        self.is_stopping.store(true, Ordering::Release);
        // the jobs with long steps all stop at once, so the joins below wait for the slowest step and not the sum
        self.workers.ask_stop();
        if let Some(task) = self.planner_task.get() {
            task.ask_stop();
        }
        for cell in &self.seal_tasks {
            if let Some(task) = cell.get() {
                task.ask_stop();
            }
        }
        if let Some(task) = self.row_task.get() {
            task.stop()?;
        }
        for cell in &self.read_tasks {
            if let Some(task) = cell.get() {
                task.stop()?;
            }
        }
        self.read_pool.refuse_queued();
        self.drain_ring();
        if let Some(task) = self.ring_task.get() {
            task.stop()?;
        }
        if let Some(task) = self.reserve_task.get() {
            task.stop()?;
        }
        self.workers.stop()?;
        if let Some(task) = self.planner_task.get() {
            task.stop()?;
        }
        for cell in &self.seal_tasks {
            if let Some(task) = cell.get() {
                task.stop()?;
            }
        }
        Ok(())
    }

    /// Close every handle of a volume
    pub(crate) fn close_volume(&self, volume: &Volume<Backend>) {
        // a read that outlives its store keeps its handle, and open finishes what the file's leaving started
        for slot in hold(&volume.parked).iter_mut() {
            if slot.as_ref().is_some_and(|parked| volume.is_unpinned(parked))
                && let Some(parked) = slot.take()
            {
                // a file that stays is one open walks dead again, and a close has no later step to try
                if let Err(parked) = self.clear_parked(volume, parked)
                    && let Some(retired) = parked.retired
                {
                    self.io.close_file(retired.file);
                }
            }
        }
        // a stopped rewrite leaves the store as it found it
        for lane in &volume.lanes {
            let mut outputs = mem::take(&mut *hold(&lane.partial_outputs));
            // the last data output lists the inputs, and if it stays on disk the next open needs the rest of its set
            let last = outputs[..MOST_MERGE_OUTPUTS].iter().rposition(Option::is_some);
            let mut is_set_gone = true;
            if let Some(Err(output)) = last.and_then(|slot| outputs[slot].take()).map(|output| self.remove_output(volume, output)) {
                is_set_gone = false;
                self.io.close_file(output.file);
            }
            for output in outputs.into_iter().flatten() {
                let left = if is_set_gone { self.remove_output(volume, output) } else { Err(output) };
                if let Err(output) = left {
                    self.io.close_file(output.file);
                }
            }
        }
        let mut claims = hold(&volume.claims);
        for segment in claims.open.drain(..) {
            self.slot_files[segment.slot.0 as usize].set(None);
            // a seal past its install left the table as the data file's owner
            if !volume.table.holds(segment.number) {
                self.io.close_file(segment.data);
            }
            self.io.close_file(segment.rows);
        }
        drop(claims);

        // a seal stopped between its switch and its clean left its rows with the slot
        for slot_file in &self.slot_files {
            if let Some(rows) = hold(&slot_file.leaving_rows).take() {
                self.io.close_file(rows);
            }
        }

        for shard in 0..TABLE_SHARDS {
            let mut guard = volume.table.shards.write(shard);
            for slot in guard.segments.iter_mut() {
                if let Some(entry) = slot.take() {
                    self.io.close_file(entry.file);
                }
            }
        }
    }
}

/// Install what the scan found, recover the open segments and start the jobs
pub fn bring_up<Backend: Io>(
    state: &Arc<EngineState<Backend>>,
    scan: &mut RootScan<Backend::File>,
) -> Result<(), StoreError> {
    // what stays on disk stays out of every list, and its going waits for this open's summary
    for held in mem::take(&mut scan.held) {
        let volume = &state.volumes[held.volume];
        if !volume.can_park(1) {
            return Err(open_error("holding a file a merge replaced")(Error::from(ErrorKind::StorageFull)));
        }
        park(volume, None, held.number, false);
    }
    // damage ranks at the counter, so the counter passes every place open can find first
    for found in &scan.sealed {
        state.resume_past(found.head.places.highest);
    }
    pass_open_rows(state, scan);
    load_covers(state, scan).map_err(open_error("reading pending range deletes"))?;
    settle_summaries(state, scan).map_err(open_error("settling a summary"))?;

    scan.sealed.reverse();
    while let Some(found) = scan.sealed.last() {
        let volume = &state.volumes[found.volume];
        state.install_sealed(volume, found).map_err(open_error("installing a segment"))?;
        scan.sealed.pop();
    }
    // the rows name finished segments and dead rows a block, which go to the table once it holds them
    for volume in &state.volumes {
        let mut waiting = 0;
        for row in hold(&volume.summary).rows() {
            if row.facts.is_walked() {
                volume.table.mark_walked(row.facts.segment);
                if row.facts.has_covers {
                    volume.covers.mark_segment_applied(row.facts.segment);
                }
            } else {
                waiting += 1;
            }
            for (block, dead) in row.dead_blocks() {
                volume.table.drop_live(row.facts.segment, block, dead, WritePlace(0));
            }
        }
        volume.walks_waiting.store(waiting, Ordering::Release);
    }
    recover_segments(state, scan).map_err(open_error("recovering a segment"))?;
    for volume in &state.volumes {
        // bits that fail to load leave their rows to ask, which costs a merge time and nothing else
        let _loaded = state.load_dead_bits(volume);
    }
    #[cfg(feature = "test-hooks")]
    if state.io.proves_dead_bits() {
        check_dead_counts(state);
    }
    state.remove_stale_images().map_err(open_error("removing an image of a column that changed kind"))?;
    state.fill_complete_tables().map_err(open_error("filling a complete table"))?;
    state.fill_sorted_indexes().map_err(open_error("filling a sorted index"))?;
    let damaged: usize = state.volumes.iter().map(|volume| hold(&volume.damaged).len()).sum();
    state.damaged_segments.store(damaged as u64, Ordering::Release);

    // some platforms set a lock up at its first use, and close is the first to take this one
    for volume in &state.volumes {
        drop(hold(&volume.locks));
    }
    drop(hold(&state.superblock));
    drop(hold(&state.superblock_bytes));

    // an open is not a state: the limits say what a run-through would seal, from the same rows
    for volume in &state.volumes {
        let mut claims = hold(&volume.claims);
        state.mark_full_segments(&mut claims, volume);
    }

    // what the walk freed in place before the stop stays off the disk, so the space target counts it gone from the open on
    for volume in &state.volumes {
        seed_freed(volume);
    }

    start_jobs(state)
}

/// Note each segment's bytes freed in place at open: what its file no longer holds, up to its dead bytes, since the counts in
/// memory start at none
pub fn seed_freed<Backend: Io>(volume: &Volume<Backend>) {
    let mut rows = hold(&volume.summary);
    let numbers: Vec<(SegmentNumber, u64, u64)> = rows
        .rows()
        .map(|row| (row.facts.segment, row_dead_bytes(&row), row.facts.file_length.0))
        .filter(|(_segment, dead, _length)| *dead > 0)
        .collect();
    for (segment, dead, length) in numbers.iter().copied() {
        let held = volume.table.pin(segment).and_then(|pin| pin.file.held_bytes().ok());
        if let Some(held) = held {
            rows.note_freed(segment, dead.min(length.saturating_sub(held.0)));
        }
    }
}

/// Start the row job, the walk workers, the planner, the reserve job, the readers and the ring's reaper
pub fn start_jobs<Backend: Io>(state: &Arc<EngineState<Backend>>) -> Result<(), StoreError> {
    let jobs_from = state.io.now();
    // a store reads busy from its jobs' start until a quiet spell passes with no call, so the first picks hold the plain bar
    for volume in &state.volumes {
        volume.last_call_at.store(jobs_from.0, Ordering::Relaxed);
    }
    let row_job = RowJob {
        state: Arc::clone(state),
        entering: EnterWatch::default(),
    };
    let task = state.io.spawn_job("reel-rows", row_job).map_err(open_error("starting a job"))?;
    // the cell is empty until here, so this set can't fail
    let _set = state.row_task.set(task);
    for volume in 0..state.volumes.len() {
        let seal_job = SealJob::new(Arc::clone(state), volume);
        state.seal_heap.fetch_add(seal_job.heap_bytes().0, Ordering::AcqRel);
        let task = state.io.spawn_job("reel-seal", seal_job).map_err(open_error("starting a job"))?;
        let _set = state.seal_tasks[volume].set(task);
    }
    let mut walks = Vec::with_capacity(state.volumes.len());
    let mut compactions = Vec::with_capacity(state.volumes.len());
    for volume in 0..state.volumes.len() {
        let walk = WalkJob::new(Arc::clone(state), volume);
        state.walk_heap.fetch_add(walk.heap_bytes().0, Ordering::AcqRel);
        walks.push(walk);
        let compaction = CompactJob::new(Arc::clone(state), volume);
        state.compaction_heap.fetch_add(compaction.heap_bytes().0, Ordering::AcqRel);
        compactions.push(compaction);
    }
    let slots: u64 = state.volumes.iter().flat_map(|volume| &volume.walk_slots).map(|slot| slot.heap_bytes().0).sum();
    state.walk_heap.fetch_add(slots, Ordering::AcqRel);
    let seats: Vec<_> = compactions.iter().map(|compaction| Arc::clone(&compaction.seats)).collect();
    for (index, cell) in state.workers.cells.iter().enumerate() {
        let worker = WalkWorker {
            state: Arc::clone(state),
            index,
            next_volume: index % state.volumes.len().max(1),
            counter: BlockCounter::new(Arc::clone(state)),
            seats: seats.clone(),
            rewrite: None,
            seat: (0, 0),
        };
        state.walk_heap.fetch_add(worker.counter.heap_bytes().0, Ordering::AcqRel);
        let (task, priority) = state.io.spawn_worker("reel-walk", worker).map_err(open_error("starting a job"))?;
        *hold(&state.worker_priority) = priority;
        let _set = cell.task.set(task);
    }
    let workers = state.workers.len() as u64;
    let planner = Planner {
        state: Arc::clone(state),
        walks,
        compactions,
        filters: (0..state.volumes.len()).map(|volume| FilterLoader::new(Arc::clone(state), volume)).collect(),
        scrubs: (0..state.volumes.len()).map(|volume| ScrubJob::new(Arc::clone(state), volume)).collect(),
        last: 0,
    };
    let task = state.io.spawn_job("reel-planner", planner).map_err(open_error("starting a job"))?;
    let _set = state.planner_task.set(task);
    let reserve_job = ReserveJob::new(Arc::clone(state));
    let task = state.io.spawn_job("reel-reserve", reserve_job).map_err(open_error("starting a job"))?;
    let _set = state.reserve_task.set(task);
    for cell in &state.read_tasks {
        let worker = ReadWorker {
            state: Arc::clone(state),
        };
        let task = state.io.spawn_job("reel-read", worker).map_err(open_error("starting a job"))?;
        let _set = cell.set(task);
    }
    if state.ring.is_some() {
        let reaper = RingReaper { state: Arc::clone(state) };
        let task = state.io.spawn_job("reel-ring", reaper).map_err(open_error("starting a job"))?;
        let _set = state.ring_task.set(task);
    }
    state.job_count.store(SHARED_JOBS + state.read_tasks.len() as u64 + state.volumes.len() as u64 + workers, Ordering::Release);
    state.prefault_time.fetch_add(state.io.now().0.saturating_sub(jobs_from.0), Ordering::AcqRel);
    state.note_graves_at_open();
    Ok(())
}

/// Describe a store to the sizing, with each volume's disk size or nothing where it has none
pub fn store_shape(config: &StoreConfig, has_ring: bool, disks: &[Option<ByteCount>]) -> StoreShape {
    let root_count = config.volumes.iter().map(|volume| volume.roots.len()).sum();
    let bare = StoreShape {
        budget: config.memory_budget,
        roots: root_count,
        open_segments: config.volumes.iter().map(VolumeConfig::most_open).sum(),
        lifetime_groups: config.volumes.iter().map(VolumeConfig::most_groups).sum(),
        volumes: config.volumes.len(),
        has_lz4: config.columns.iter().any(|column| column.codec == Codec::Lz4),
        widest_keys: (0..config.volumes.len()).map(|volume| config.widest_key(volume) as u64).sum(),
        threads: config.threads,
        has_ring,
        disk_entry_bytes: ByteCount(0),
        floor_entry_bytes: entry_line(config, &disks.iter().map(|disk| disk.map(|_size| floor_entries())).collect::<Vec<_>>()),
        workers: None,
        declared_bytes: declared_index_bytes(config),
        is_read_only: false,
        // the slots' windows serve every volume, so they hold the largest any volume takes
        window_bytes: config.volumes.iter().map(|volume| window_bytes_for(volume.segment_bytes.map(|bytes| bytes.0))).max().unwrap_or(0),
        // each volume's seal plans its list at the blocks its segments can need
        seal_entries_saved: (0..config.volumes.len()).map(|volume| u64::from(MOST_SEGMENT_BLOCKS.0 - seal_blocks(config, volume).0) * size_of::<BlockEntry>() as u64).sum(),
    };
    // a segment's rows come from the shares, so the disks ask at the rows a plan with no line of theirs gives
    let rows = EngineSizing::for_store(&bare.with_workers_settled()).segment_rows;
    StoreShape { disk_entry_bytes: entry_line(config, &asked_entries(config, disks, rows)), ..bare }
}

/// Add up the heap a config's complete tables hold with their block lines, by the arithmetic they are built with. Sorted indexes
/// take their room from the caches at open
pub fn declared_index_bytes(config: &StoreConfig) -> ByteCount {
    let tables: u64 = config.columns.iter().filter_map(ColumnConfig::complete_rows).map(|rows| planned_table_bytes(rows).0).sum();
    ByteCount(tables + complete_lines(config) * size_of::<BlockEntry>() as u64)
}

/// Count the columns a volume holds
pub fn columns_on(config: &StoreConfig, volume: usize) -> usize {
    config.columns.iter().filter(|column| column.volume.0 as usize == volume).count()
}

/// Add up the bytes of the volumes' entries, at their columns' cost
pub fn entry_line(config: &StoreConfig, entries: &[Option<usize>]) -> ByteCount {
    let bytes = |(volume, count): (usize, &Option<usize>)| Some(count.as_ref().copied()? as u64 * entry_cost(columns_on(config, volume)));
    ByteCount(entries.iter().enumerate().filter_map(bytes).sum())
}

/// Read the segment size a volume's entries plan at, from the rows a segment takes
pub fn volume_segment_bytes(config: &StoreConfig, volume: usize, rows: RowCount) -> ByteCount {
    assumed_segment_bytes(config.volumes[volume].segment_bytes.unwrap_or_else(|| segment_bytes_for(config.memory_budget)), rows, config.row_floor(volume))
}

/// Read the entries each volume's disk asks for at its segment size, or nothing for a volume with a disk of no known size
pub fn asked_entries(config: &StoreConfig, disks: &[Option<ByteCount>], rows: RowCount) -> Vec<Option<usize>> {
    disks.iter().enumerate().map(|(volume, disk)| disk_entries(*disk, volume_segment_bytes(config, volume, rows))).collect()
}

/// Plan each volume's entries inside the planned line: what its disk asks for, with every volume's entries past the floor scaled
/// down together when the line is short of what they ask
pub fn volume_entries(config: &StoreConfig, disks: &[Option<ByteCount>], rows: RowCount, line: ByteCount) -> Vec<Option<usize>> {
    let entries = asked_entries(config, disks, rows);
    let asked = entry_line(config, &entries).0;
    if asked <= line.0 {
        return entries;
    }
    let floor = entry_line(config, &entries.iter().map(|count| count.map(|_count| floor_entries())).collect::<Vec<_>>()).0;
    let room = line.0.saturating_sub(floor);
    entries.iter().map(|count| count.map(|count| capped_entries(count, room, asked - floor))).collect()
}

/// Read each volume's disk size: the sizes of the distinct filesystems its roots sit on, or nothing when a root's disk has no
/// known size
pub fn volume_disks<Backend: Io>(io: &Backend, config: &StoreConfig) -> Result<Vec<Option<ByteCount>>, Error> {
    let mut disks = Vec::with_capacity(config.volumes.len());
    for volume in &config.volumes {
        let mut filesystems = Vec::with_capacity(volume.roots.len());
        let mut total = Some(0);
        for root in &volume.roots {
            match io.capacity(root)? {
                Some(disk) if !filesystems.contains(&disk.filesystem) => {
                    filesystems.push(disk.filesystem);
                    total = total.map(|bytes: u64| bytes.saturating_add(disk.bytes.0));
                },
                Some(_counted) => {},
                None => total = None,
            }
        }
        disks.push(total.map(ByteCount));
    }
    Ok(disks)
}

/// Check a store runs async reads and its budget holds the ring beside the least it runs on. A smaller one keeps async reads on
/// the blocking pool
pub fn ring_fits(config: &StoreConfig, disks: &[Option<ByteCount>]) -> bool {
    // the ring serves async reads alone, and a store with no read thread answers none
    if config.threads.read_threads == 0 {
        return false;
    }
    let shape = StoreShape { workers: Some(0), ..store_shape(config, true, disks) };
    shape.least_budget() <= config.memory_budget
}

/// Share a budget out for a store, or say why it can't run on it
pub fn store_sizing(config: &StoreConfig, has_ring: bool, disks: &[Option<ByteCount>]) -> Result<EngineSizing, StoreError> {
    sizing_of(config, store_shape(config, has_ring, disks))
}

/// Size a read-only open: the reader's own parts, and none a writer holds for writing
pub fn read_only_sizing(config: &StoreConfig, disks: &[Option<ByteCount>]) -> Result<EngineSizing, StoreError> {
    sizing_of(config, StoreShape { is_read_only: true, ..store_shape(config, false, disks) })
}

/// Size a store of this shape, or refuse a budget or a thread count it can't hold
pub fn sizing_of(config: &StoreConfig, shape: StoreShape) -> Result<EngineSizing, StoreError> {
    let shape = shape.with_workers_settled();
    let least_shape = StoreShape { workers: Some(0), ..shape };
    if config.memory_budget >= least_shape.least_budget() && config.memory_budget < shape.least_budget() {
        return Err(StoreError::ThreadsPastBudget {
            maintenance_workers: shape.workers(),
            read_threads: config.threads.read_threads,
            budget: config.memory_budget,
            least: shape.least_budget(),
        });
    }
    if config.memory_budget < shape.least_budget() {
        return Err(StoreError::BudgetTooSmall {
            budget: config.memory_budget,
            least: shape.least_budget(),
        });
    }
    Ok(EngineSizing::for_store(&shape))
}

/// Refuse a config that can't work
pub fn check_config(config: &StoreConfig) -> Result<(), StoreError> {
    for volume in &config.volumes {
        if let Some(asked) = volume.segment_bytes.filter(|asked| *asked < LEAST_SEGMENT_BYTES || *asked > MOST_SET_SEGMENT_BYTES) {
            return Err(StoreError::SegmentSize { asked, least: LEAST_SEGMENT_BYTES, most: MOST_SET_SEGMENT_BYTES });
        }
    }
    for volume in &config.volumes {
        if !volume.roots.iter().all(|root| fits_a_segment_path(root)) {
            return Err(StoreError::Io {
                action: "checking a root",
                source: Error::other("the path leaves no room for a segment file"),
            });
        }
    }

    let mut has_lz4 = false;
    let mut seen = [false; MAX_COLUMNS];
    for column in &config.columns {
        // a sorted index holds keys of one width, and a complete table holds the rows it was given
        let has_fixed_keys = matches!(column.key_shape, KeyShape::Fixed { width } if (1..=MOST_KEY_BYTES).contains(&usize::from(width)));
        if column.index == ColumnIndex::Sorted && !has_fixed_keys {
            return Err(StoreError::BadIndex { column: column.name.clone(), reason: "a sorted index takes keys of one fixed width" });
        }
        if column.index == ColumnIndex::Complete && column.least_rows.is_none() {
            return Err(StoreError::BadIndex { column: column.name.clone(), reason: "a complete table takes least_rows, the rows it holds" });
        }
        if seen[column.id.0 as usize] || !column.key_shape.is_valid() || (column.is_sorted() && !has_fixed_keys) {
            return Err(column_changed(&column.name, "declaration"));
        }
        seen[column.id.0 as usize] = true;
        if column.volume.0 as usize >= config.volumes.len() {
            return Err(column_changed(&column.name, "volume"));
        }
        has_lz4 |= column.codec == Codec::Lz4;
    }

    if has_lz4 && config.memory_budget < LEAST_LZ4_BUDGET {
        return Err(StoreError::Io {
            action: "sizing the compress pool",
            source: Error::other("budget can't hold the compress pool an lz4 column needs"),
        });
    }
    Ok(())
}

/// Build the error for a column the config changed
pub fn column_changed(name: &str, field: &'static str) -> StoreError {
    StoreError::ColumnChanged {
        name: name.to_string(),
        field,
    }
}

/// Take the lock of every root, and name the process that holds any of them
pub fn lock_roots<Backend: Io>(io: &Backend, config: &StoreConfig) -> Result<Vec<Vec<Backend::Lock>>, StoreError> {
    let mut volumes = Vec::with_capacity(config.volumes.len());
    for volume in &config.volumes {
        let mut locks = Vec::with_capacity(volume.roots.len());
        for root in &volume.roots {
            io.create_directory(root).map_err(open_error("creating a root"))?;
            let outcome = io.lock(&root.join(LOCK_NAME)).map_err(open_error("locking a root"))?;
            match outcome {
                LockOutcome::Held(lock) => locks.push(lock),
                LockOutcome::Refused { holder } => {
                    return Err(StoreError::Locked {
                        root: root.clone(),
                        holder,
                    });
                },
            }
        }
        volumes.push(locks);
    }
    Ok(volumes)
}

/// Read the newest superblock of any root, check the config against it, and write it to every root
/// Read the newest superblock every root agrees on, and the highest limit any copy holds. No copy anywhere that passes, with
/// some that exist, is a lost store and never a new one
pub fn newest_superblock<Backend: Io>(io: &Backend, config: &StoreConfig) -> Result<(Option<Superblock>, WritePlace), StoreError> {
    let mut newest: Option<Superblock> = None;
    let mut highest_limit = WritePlace(0);
    let mut has_failed = false;
    for volume in &config.volumes {
        for root in &volume.roots {
            let copies = RootCopies::read(io, root).map_err(open_error("reading a superblock"))?;
            has_failed |= copies.has_failed();
            for copy in copies.passed() {
                highest_limit = highest_limit.max(copy.place_limit);
            }
            let found = copies.newest();
            let Some(found) = found else {
                continue;
            };
            let is_newer = match &newest {
                Some(current) => {
                    if current.store_id != found.store_id {
                        return Err(StoreError::Io {
                            action: "reading superblocks",
                            source: Error::other("roots belong to different stores"),
                        });
                    }
                    found.revision > current.revision
                },
                None => true,
            };
            if is_newer {
                newest = Some(found);
            }
        }
    }

    if newest.is_none() && has_failed {
        return Err(StoreError::Io {
            action: "reading a superblock",
            source: Error::from(ErrorKind::InvalidData),
        });
    }

    Ok((newest, highest_limit))
}

/// Read what a read-only open goes by, and write nothing
pub fn read_superblock<Backend: Io>(io: &Backend, config: &StoreConfig) -> Result<Superblock, StoreError> {
    let (newest, _limit) = newest_superblock(io, config)?;
    let Some(superblock) = newest else {
        return Err(StoreError::Io {
            action: "reading a superblock",
            source: Error::from(ErrorKind::NotFound),
        });
    };
    check_layout(&superblock, config)?;
    Ok(superblock)
}

/// Read the superblock, or start one for a new store, and save it with the next run of places
pub fn settle_superblock<Backend: Io>(io: &Backend, config: &StoreConfig) -> Result<Superblock, StoreError> {
    let (newest, highest_limit) = newest_superblock(io, config)?;
    let mut superblock = match newest {
        Some(superblock) => {
            check_layout(&superblock, config)?;
            superblock
        },
        None => Superblock {
            store_id: StoreId(io.random_word()),
            record_key: RecordKey(io.random_word()),
            revision: 0,
            next_segment: SegmentNumber(1),
            place_limit: WritePlace(0),
            volumes: Vec::new(),
            columns: Vec::new(),
            dead_roots: Vec::new(),
        },
    };

    superblock.revision += 1;
    superblock.volumes = stored_volumes(config);
    superblock.columns = stored_columns(config, &superblock.columns);
    superblock.next_segment = SegmentNumber(superblock.next_segment.0 + SEGMENT_NUMBER_BATCH);
    superblock.place_limit = WritePlace(highest_limit.0 + PLACE_RUN);

    let mut bytes = Vec::new();
    for volume in &config.volumes {
        for root in &volume.roots {
            let paths = SuperblockPaths::new(root);
            superblock
                .write(io, &paths, &mut bytes)
                .map_err(open_error("writing a superblock"))?;
        }
    }
    Ok(superblock)
}

/// Refuse a config that dropped a root or changed a column
pub fn check_layout(superblock: &Superblock, config: &StoreConfig) -> Result<(), StoreError> {
    let layout_error = |message: &str| StoreError::Io {
        action: "checking the volume layout",
        source: Error::other(message.to_string()),
    };
    // a root is matched by its volume and its place in the volume, so a store opens wherever its files were copied. Roots of
    // two stores are refused where the superblocks are read, by their store ids
    for stored in &superblock.volumes {
        let Some(volume) = config.volumes.iter().find(|volume| volume.name == stored.name) else {
            return Err(layout_error("config dropped a volume"));
        };
        if volume.roots.len() < stored.roots.len() {
            return Err(layout_error("config dropped a root"));
        }
    }

    for stored in &superblock.columns {
        let Some(column) = config.columns.iter().find(|column| column.name == stored.name) else {
            continue;
        };
        check_column(stored, column, config)?;
    }
    Ok(())
}

/// Refuse a changed id, key shape or volume
pub fn check_column(stored: &StoredColumn, column: &ColumnConfig, config: &StoreConfig) -> Result<(), StoreError> {
    if stored.id != column.id {
        return Err(column_changed(&column.name, "id"));
    }
    if stored.key_shape != column.key_shape {
        return Err(column_changed(&column.name, "key shape"));
    }
    if config.volumes[column.volume.0 as usize].name != stored.volume_name {
        return Err(column_changed(&column.name, "volume"));
    }
    Ok(())
}

/// List the config's volumes as a superblock keeps them
pub fn stored_volumes(config: &StoreConfig) -> Vec<StoredVolume> {
    let mut volumes = Vec::with_capacity(config.volumes.len());
    for volume in &config.volumes {
        let mut roots = Vec::with_capacity(volume.roots.len());
        for root in &volume.roots {
            roots.push(root.to_string_lossy().into_owned());
        }
        volumes.push(StoredVolume {
            name: volume.name.clone(),
            roots,
        });
    }
    volumes
}

/// List the config's columns as a superblock keeps them
pub fn stored_columns(config: &StoreConfig, _earlier: &[StoredColumn]) -> Vec<StoredColumn> {
    let mut columns = Vec::with_capacity(config.columns.len());
    for column in &config.columns {
        let volume = &config.volumes[column.volume.0 as usize];
        columns.push(stored_column(column, &volume.name));
    }
    columns
}

/// Give a table the planned room, or more when the roots already hold more
pub fn table_room(planned: &TableSizing, found: &TableSizing) -> TableSizing {
    TableSizing {
        segments_per_shard: planned.segments_per_shard.max(found.segments_per_shard * 2),
        columns_per_shard: planned.columns_per_shard.max(found.columns_per_shard * 2),
        blocks_per_shard: planned.blocks_per_shard.max(found.blocks_per_shard * 2),
    }
}

/// Work out the most a row can take on a volume with this many columns
pub fn row_room(columns: usize) -> usize {
    MOST_ROW_FIXED_BYTES + columns * MOST_ROW_COLUMN_BYTES
}

/// Plan a volume's table: the more of what half its part of the share buys and what its disk plans for, and its block-index
/// lines from the rest of that part
pub fn planned_table(config: &StoreConfig, entries: Option<usize>, volume: usize, volume_share: u64) -> TableSizing {
    let columns = columns_on(config, volume).max(1);
    let cost = entry_cost(columns);
    // the disk only adds entries to what half the share buys: the line pays the disk's, the share pays any past them, and the
    // block-index lines take the rest of the share
    let from_disk = entries.unwrap_or(0);
    let entries = ((volume_share / 2 / cost) as usize).max(from_disk);
    let line_share = volume_share - (entries - from_disk) as u64 * cost;
    let segments_per_shard = (entries / TABLE_SHARDS).max(LEAST_SHARD_ENTRIES);
    TableSizing {
        segments_per_shard,
        columns_per_shard: segments_per_shard * columns,
        blocks_per_shard: ((line_share / PLANNED_LINE_BYTES) as usize / TABLE_SHARDS).max(64),
    }
}

/// What open read before it builds the engine's state
pub struct OpenInputs<'open, Backend: Io> {
    pub io: Backend,
    pub config: &'open StoreConfig,
    pub sizing: &'open EngineSizing,
    pub disks: &'open [Option<ByteCount>],
    pub superblock: Superblock,
    pub locks: Vec<Vec<Backend::Lock>>,
    pub scan: &'open RootScan<Backend::File>,
    pub summaries: Vec<SummaryFound>,
}

/// Allocate and touch everything the engine will hold
pub fn build_state<Backend: Io>(inputs: OpenInputs<'_, Backend>) -> EngineState<Backend> {
    let OpenInputs { io, config, sizing, disks, superblock, mut locks, scan, summaries } = inputs;
    // the write index holds what a segment of the store's mean record needs, and the record cache takes the rest
    let mean = mean_record(&summaries);
    let cache_mean = cacheable_mean(&summaries);
    let sizing = sizing.for_mean_record(mean, config.volumes.len() as u64);
    // a store whose every volume set its segment size holds no more rows than those segments can, a segment's bytes over the
    // format's row floor. A column's declared least value is no floor here, since a smaller value lands and a delete has none
    let set_rows = set_segment_rows(config);
    let sizing = sizing.for_set_segments(set_rows, config.volumes.len() as u64);
    // a reader holds the open rows its listing found with room past them, at every open and refresh
    let open_rows = RowCount(scan.scanned.iter().map(|scanned| scanned.scan.row_count.0).sum());
    let sizing = &if sizing.is_read_only { sizing.for_open_rows(open_rows) } else { sizing };
    let first_place = superblock.place_limit.0.saturating_sub(PLACE_RUN);
    let places = Arc::new(PlaceCount::new(WritePlace(first_place), superblock.place_limit));
    let volume_share = sizing.table_share.0 / config.volumes.len().max(1) as u64;
    let entries = volume_entries(config, disks, sizing.segment_rows, sizing.entry_line);
    let planned: Vec<TableSizing> =
        (0..config.volumes.len()).map(|volume| planned_table(config, entries.get(volume).copied().flatten(), volume, volume_share)).collect();

    let mut columns: Vec<Option<ColumnInfo>> = vec![None; MAX_COLUMNS];
    let mut directories: Vec<Option<ColumnDirectory>> = Vec::with_capacity(MAX_COLUMNS);
    for _column in 0..MAX_COLUMNS {
        directories.push(None);
    }
    // the columns of a volume have the same room, so they share one spare
    let mut spares: Vec<Option<Arc<Mutex<DirectoryList>>>> = vec![None; config.volumes.len()];
    let mut has_lz4 = false;
    for column in &config.columns {
        has_lz4 |= column.codec == Codec::Lz4;
        columns[column.id.0 as usize] = Some(ColumnInfo {
            key_shape: column.key_shape,
            codec: column.codec,
            volume: column.volume,
            is_complete: column.complete_rows().is_some(),
            least_value: column.least_value.map_or(0, |bytes| bytes.0),
        });
        let found = scan.sealed.len();
        let room = (planned[column.volume.0 as usize].segments_per_shard * TABLE_SHARDS).max(found * 2);
        let spare = spares[column.volume.0 as usize].get_or_insert_with(|| Arc::new(Mutex::new(empty_list(room))));
        directories[column.id.0 as usize] = Some(ColumnDirectory::with_spare(room, Arc::clone(spare)));
    }

    let open_slots = sizing.shard.open_slots;
    // a reader writes no group, so its slots hold no queue and no spare
    let group_bytes = if sizing.is_read_only { ByteCount(0) } else { GROUP_BYTES };
    // nor does it fill a record window, so its windows hold no buffer
    let window_bytes = if sizing.is_read_only { 0 } else { sizing.window_bytes };
    let mut volumes = Vec::with_capacity(config.volumes.len());
    let all_row_room = config.volumes.len() * MOST_ROW_FIXED_BYTES + config.columns.len() * MOST_ROW_COLUMN_BYTES;
    let mut summaries = summaries.into_iter();
    let progress = Arc::new(WakeLine::new());
    let context = VolumeContext { io: &io, config, sizing, disks, scan, places: &places, planned: &planned, open_slots, all_row_room, progress: &progress };
    for (index, held) in locks.iter_mut().map(mem::take).enumerate() {
        let Some(summary) = summaries.next() else {
            continue;
        };
        volumes.push(build_volume(&context, index, held, summary));
    }

    let mut queues = Vec::with_capacity(open_slots);
    let mut spare_groups = Vec::with_capacity(open_slots);
    let mut windows = Vec::with_capacity(open_slots);
    let mut window_copies = Vec::with_capacity(open_slots);
    let mut slot_files = Vec::with_capacity(open_slots);
    let mut free_slots = Vec::with_capacity(open_slots);
    for slot in 0..open_slots {
        queues.push(Mutex::new(RowQueue::new(group_bytes)));
        spare_groups.push(Mutex::new((!sizing.is_read_only).then(|| RowGroupBuffer::new(GROUP_BYTES))));
        windows.push([RecordWindow::with_bytes(window_bytes), RecordWindow::with_bytes(window_bytes)]);
        window_copies.push(Mutex::new(touched_zeroes(window_bytes)));
        slot_files.push(SlotFile::new());
        free_slots.push(OpenSlot((open_slots - 1 - slot) as u8));
    }
    // a mutex's first lock can allocate, and a slot first used after open allocates nothing
    for slot in 0..open_slots {
        drop(hold(&queues[slot]));
        drop(hold(&spare_groups[slot]));
        drop(hold(&window_copies[slot]));
    }

    // a value reaches the largest segment any volume takes, which a set size can raise past the budget's rule
    let largest_value = config.volumes.iter().filter_map(|volume| volume.segment_bytes).fold(sizing.segment_bytes, ByteCount::max);
    let most_shares = if has_lz4 && !sizing.is_read_only { sizing.lz4_stored_limit.0 as usize / SHARE_BYTES } else { 0 };
    let counts = sizing.counts;
    let mut encoders = Vec::with_capacity(counts.write_buffers);
    for _encoder in 0..counts.write_buffers {
        encoders.push(RecordEncoder::new(largest_value, most_shares));
    }
    let shares = (0..most_shares).map(|_share| touched_zeroes(SHARE_BYTES)).collect();

    let readers = counts.read_buffers;
    let mut record_readers = Vec::with_capacity(readers);
    let widest_key = (0..config.volumes.len()).map(|volume| config.widest_key(volume)).max().unwrap_or(0);
    let scans: Vec<ScanState> = (0..counts.scans).map(|_scan| ScanState::new(widest_key, open_slots)).collect();
    let scan_heap = ByteCount(scans.iter().map(|scan| scan.heap_bytes().0).sum());
    let mut footer_readers = Vec::with_capacity(readers);
    for _reader in 0..readers {
        record_readers.push(RecordReader::new());
        footer_readers.push(FooterReader::new());
    }

    let mut seal_tasks = Vec::with_capacity(config.volumes.len());
    for _volume in &config.volumes {
        seal_tasks.push(OnceLock::new());
    }

    let filter_share = ByteCount(sizing.cache_share.0 / 4 * FILTER_CACHE_QUARTERS);
    // the cache takes whole stripes, so the arena works in them too and the pair never holds more than the cache alone would
    let stripe = (CACHE_SHARDS * CACHE_WAYS * FILTER_SLOT_BYTES) as u64;
    let whole = filter_share.0 / stripe * stripe;
    // filters come before the record cache: what the filter share can't hold of the arena comes out of the record cache's share,
    // after the dead bits, and the record cache keeps what is left
    let rows_on_disk: u64 = volumes.iter().map(|volume| hold(&volume.summary).rows().map(|row| rows_of(&row)).sum::<u64>()).sum();
    let arena_wanted = target_rows(rows_on_disk).saturating_mul(BITS_PER_ROW as u64) / 8;
    // the arena takes what the target rows need from the filter share first, in whole stripes, and the filter cache keeps the rest
    let cache_room = whole.saturating_sub(arena_wanted).max(stripe).next_multiple_of(stripe);
    let base_arena = whole.saturating_sub(cache_room);
    let dead_bits: u64 = volumes.iter().map(|volume| volume.dead_bits.heap_bytes().0).sum();
    let record_room = sizing.record_share.0.saturating_sub(dead_bits);
    let arena_taken = arena_wanted.saturating_sub(base_arena).min(record_room.saturating_sub(RecordCache::least_bytes()));
    let arena_share = ByteCount(base_arena + arena_taken);
    let is_complete = config.columns.iter().any(|column| column.complete_rows().is_some());
    let block_share = ByteCount(sizing.cache_share.0 - filter_share.0);
    // complete columns read small blocks, and a slot sized for a full block would hold a sixteenth of what it could
    let complete_share = ByteCount(if is_complete { block_share.0 / 2 } else { 0 });
    let base_rows = block_share.0 - complete_share.0;
    let row_blocks: u64 = volumes.iter().map(|volume| hold(&volume.summary).rows().map(|row| u64::from(row.facts.block_count)).sum::<u64>()).sum();
    let split = split_idle_room(cache_room, stripe, base_rows, row_blocks * ROW_BLOCK_BYTES as u64);
    let row_share = ByteCount(base_rows + split.to_rows);
    let record_share = ByteCount(record_room - arena_taken + split.to_records);
    let reserved = superblock.next_segment.0;
    let first_number = reserved.saturating_sub(SEGMENT_NUMBER_BATCH);

    // a sorted column's rows count apart from the start, so its walks see only the rows its index lacks
    let index = WriteIndex::new(&sizing.shard);
    for column in config.columns.iter().filter(|column| column.is_sorted()) {
        index.mark_sorted(column.id);
    }
    let opened_at = io.now();
    EngineState {
        io,
        record_key: superblock.record_key,
        sizing: *sizing,
        columns,
        directories,
        volumes,
        places,
        index,
        spot: SpotIndex::new(sizing.spot_share),
        filter_cache: BlockCache::new(ByteCount(split.filter_cache), FILTER_SLOT_BYTES),
        filter_arena: FilterArena::new(arena_share, planned.iter().map(|table| table.segments_per_shard * TABLE_SHARDS).sum()),
        row_cache: BlockCache::new(row_share, ROW_BLOCK_BYTES),
        record_cache: RecordCache::new(record_share, cache_mean),
        complete_cache: is_complete.then(|| BlockCache::new(complete_share, COMPLETE_BLOCK_BYTES)),
        complete_line_bytes: ByteCount(complete_lines(config) * size_of::<BlockEntry>() as u64),
        queues,
        spare_groups,
        windows,
        window_copies,
        slot_files,
        free_slots: Mutex::new(free_slots),
        encoders: Pool::new(encoders),
        stored_shares: Pool::new(shares),
        batch_rows: Pool::new((0..counts.write_buffers).map(|_buffer| RowGroupBuffer::new(GROUP_BYTES)).collect()),
        batch_records: Pool::new((0..counts.write_buffers).map(|_buffer| touched_zeroes(GATHER_BYTES)).collect()),
        batch_lengths: Pool::new(vec![touched_filled(if sizing.is_read_only { 0 } else { sizing.segment_rows.0 as usize }, 0)]),
        current_mark: AtomicU64::new(0),
        longest_turn: AtomicU64::new(0),
        index_waits: AtomicU64::new(0),
        longest_index_wait: AtomicU64::new(0),
        values_under_least: (0..MAX_COLUMNS).map(|_column| AtomicU64::new(0)).collect(),
        open_time: AtomicU64::new(0),
        prefault_time: AtomicU64::new(0),
        huge_pages: AtomicU64::new(0),
        record_readers: Pool::new(record_readers),
        footer_readers: Pool::new(footer_readers),
        // each volume's planner and each of the store's workers, each holding one at a time
        job_footer_readers: Pool::new((0..config.volumes.len() + sizing.workers).map(|_reader| FooterReader::new()).collect()),
        scan_heap,
        // a reservation rewrites these bytes, and the first must not grow the buffer on a job thread
        superblock_bytes: Mutex::new(superblock.to_bytes()),
        superblock: Mutex::new(superblock),
        next_segment: AtomicU64::new(first_number),
        reserved_segment: AtomicU64::new(reserved),
        is_reserving: AtomicBool::new(false),
        deletes: AtomicU64::new(0),
        row_job_idle: AtomicBool::new(false),
        damaged_segments: AtomicU64::new(0),
        dropped_summary_rows: AtomicU64::new(0),
        tallies: Default::default(),
        written: Default::default(),
        pauses: Default::default(),
        longest_room_step: AtomicU64::new(0),
        longest_walkless_steps: AtomicU64::new(0),
        longest_walkless_kind: AtomicU64::new(0),
        cursor_lookups: Default::default(),
        merge_lookups: Default::default(),
        block_lookups: Default::default(),
        units: Default::default(),
        opened_at,
        graves_at_open: AtomicU64::new(0),
        graves_dropped: AtomicU64::new(0),
        graves_carried: AtomicU64::new(0),
        empty_rewrites: AtomicU64::new(0),
        corruption_errors: AtomicU64::new(0),
        batched_gets: AtomicU64::new(0),
        damaged_sums: AtomicU64::new(0),
        scan_seats: AtomicU64::new(0),
        seats_together: AtomicU64::new(0),
        heads_together: AtomicU64::new(0),
        merge_check_failures: AtomicU64::new(0),
        scrub_rate: config.threads.scrub_bytes_per_second,
        scrub_bytes: AtomicU64::new(0),
        scrub_passes: AtomicU64::new(0),
        scrub_failures: AtomicU64::new(0),
        ahead_drops: AtomicU64::new(0),
        scan_cache_hits: AtomicU64::new(0),
        scan_held_values: AtomicU64::new(0),
        heads_skipped: AtomicU64::new(0),
        sorted_answers: AtomicU64::new(0),
        sorted_footer_asks: AtomicU64::new(0),
        sorted_scan_keys: AtomicU64::new(0),
        sorted_runs: AtomicU64::new(0),
        scan_window_records: AtomicU64::new(0),
        scan_trusted_records: AtomicU64::new(0),
        scan_restored_values: AtomicU64::new(0),
        index_counts: AtomicU64::new(0),
        rows_entered: AtomicU64::new(0),
        scan_batches_presorted: AtomicU64::new(0),
        sorted_moves_missed: AtomicU64::new(0),
        sorted_fill_ns: AtomicU64::new(0),
        sorted_fill_bytes: AtomicU64::new(0),
        data_syncs: AtomicU64::new(0),
        caller_ops: CallerCounts::default(),
        progress,
        is_arena_full: AtomicBool::new(false),
        arena_fill: AtomicU64::new(0),
        unlinks_refused: AtomicU64::new(0),
        memory_budget: config.memory_budget,
        seal_heap: AtomicU64::new(0),
        walk_heap: AtomicU64::new(0),
        worker_priority: Mutex::new(WorkerPriority::default()),
        pick_holds: Default::default(),
        workers: Workers::new(sizing.workers),
        compaction_heap: AtomicU64::new(0),
        admission: Admission::default(),
        job_count: AtomicU64::new(0),
        row_task: OnceLock::new(),
        reserve_task: OnceLock::new(),
        read_tasks: (0..config.threads.read_threads).map(|_worker| OnceLock::new()).collect(),
        read_pool: ReadPool::new(scans, counts.read_slots, config.threads.read_threads),
        ring: None,
        ring_task: OnceLock::new(),
        ring_record_readers: Pool::new(Vec::new()),
        ring_footer_readers: Pool::new(Vec::new()),
        is_read_only: false,
        complete: Vec::new(),
        sorted: Vec::new(),
        sorted_sizes: Vec::new(),
        complete_images: Vec::new(),
        seal_tasks,
        planner_task: OnceLock::new(),
        index_waiters: AtomicU64::new(0),
        cues: CueTable::new(config.volumes.len()),
        is_stopping: AtomicBool::new(false),
    }
}

/// What every volume's state is built from, which open read once for the whole store
pub struct VolumeContext<'open, Backend: Io> {
    pub io: &'open Backend,
    pub config: &'open StoreConfig,
    pub sizing: &'open EngineSizing,
    pub disks: &'open [Option<ByteCount>],
    pub scan: &'open RootScan<Backend::File>,
    pub places: &'open Arc<PlaceCount>,
    pub planned: &'open [TableSizing],
    pub open_slots: usize,
    pub all_row_room: usize,
    pub progress: &'open Arc<WakeLine>,
}

/// Build one volume's state: its summary's room, its table, and every count it keeps
pub fn build_volume<Backend: Io>(context: &VolumeContext<'_, Backend>, index: usize, held: Vec<Backend::Lock>, mut summary: SummaryFound) -> Volume<Backend> {
    let VolumeContext { io, config, sizing, disks, scan, places, planned, open_slots, all_row_room, progress } = *context;
    let volume = &config.volumes[index];
    let mut paths = Vec::with_capacity(volume.roots.len());
    for root in &volume.roots {
        paths.push(SuperblockPaths::new(root));
    }
    let found_summary = summary.rows.bytes().len() * 2;
    let columns_here = config.columns.iter().filter(|column| column.volume.0 as usize == index).count();
    // the share splits by each volume's row size, so every volume plans the same number of rows
    // the share pays for the rows and the buffer they save through
    let planned_summary = sizing.summary_share.0 as usize / 2 * row_room(columns_here) / all_row_room;
    // open never drops a row, so a segment's damage keeps its rank through every open
    let span_room = MOST_DAMAGED_SPANS * (MOST_ROW_SPAN_FIXED_BYTES + 2 * SPAN_KEY_BYTES);
    let summary_room = span_room + found_summary.max(planned_summary).max(scan.least_summary_room(index, columns_here) * 2);
    summary.rows.make_room(ByteCount(summary_room as u64));
    let mut room = table_room(&planned[index], &scan.least_sizing(index));
    let cover_segment = volume_segment_bytes(config, index, sizing.segment_rows);
    room.blocks_per_shard += complete_shard_lines(config, index);
    // a row for each segment the table can hold, beside open segments and the spans of gone ones
    summary.rows.make_index(room.segments_per_shard * TABLE_SHARDS + open_slots + MOST_DAMAGED_SPANS);
    // the bits are sized from this volume's rows on disk with room to grow, and never past half the record cache's share
    let rows_here: u64 = summary.rows.rows().map(|row| rows_of(&row)).sum();
    let dead_share = ByteCount(dead_bits_bytes(rows_here).min(sizing.record_share.0 / 2 / config.volumes.len() as u64));
    let open_place = summary.rows.rows().map(|row| row.facts.places.highest).max().unwrap_or(WritePlace(0));
    let open_segment = summary.rows.rows().map(|row| row.facts.segment).max().unwrap_or(SegmentNumber(0));
    Volume {
        dead_bits: DeadBits::new(dead_share, room.segments_per_shard * TABLE_SHARDS),
        id: VolumeId(index as u8),
        name: volume.name.clone(),
        roots: volume.roots.clone(),
        superblock_paths: paths,
        policy: volume.sync,
        locks: Mutex::new(held),
        claims: Mutex::new(ClaimState {
            open: touched_room(open_slots),
            is_creating: false,
            prepared: None,
            wants_prepared: false,
            next_root: 0,
        }),
        table: SegmentTable::new(&room),
        view: AtomicU64::new(0),
        handovers: AtomicU64::new(0),
        highest_sealed: AtomicU64::new(0),
        sync: Mutex::new(SyncState::default()),
        is_failed: AtomicBool::new(false),
        most_open: volume.most_open(),
        most_groups: volume.most_groups(),
        segment_bytes: volume.segment_bytes.unwrap_or(sizing.segment_bytes),
        // a set size takes an eighth of itself a step, and the budget's rule keeps its floor
        reserve_step: volume.segment_bytes.map_or_else(|| reserve_step(sizing.segment_bytes), set_reserve_step),
        window_bytes: window_bytes_for(volume.segment_bytes.map(|bytes| bytes.0)),
        seal_blocks: seal_blocks(config, index),
        seal_rows: rows_within_blocks(seal_blocks(config, index), config.widest_key(index)),
        segment_rows: volume_rows(config, index, sizing.segment_rows),
        walk_retries: AtomicU64::new(0),
        walks_waiting: AtomicU64::new(0),
        wants_compaction: AtomicBool::new(true),
        has_kept_covers: AtomicBool::new(false),
        #[cfg(feature = "test-hooks")]
        holds: TestHolds::default(),
        is_seal_stuck: AtomicBool::new(false),
        stuck_shard: AtomicUsize::new(NO_SHARD),
        seal_stops: AtomicU64::new(0),
        disk_bytes: disks.get(index).copied().flatten(),
        disk_covered: disks.get(index).copied().flatten().map_or(ByteCount(0), |disk| entry_cover(room.segments_per_shard * TABLE_SHARDS, cover_segment, disk)),
        cover_segment_bytes: cover_segment,
        entry_bytes: ByteCount(entry_cost(columns_here)),
        picks: Default::default(),
        rewrite_inputs_gone: AtomicU64::new(0),
        rewrite_written: AtomicU64::new(0),
        rewrite_written_by_share: Default::default(),
        rewrite_footers_by_share: Default::default(),
        rewrite_gone_by_share: Default::default(),
        rewrites_by_share: Default::default(),
        unlinked_bytes: AtomicU64::new(0),
        rewrite_dead_gone: AtomicU64::new(0),
        rewrite_dead_kept: AtomicU64::new(0),
        rewrites_unreclaimed: AtomicU64::new(0),
        walk_blocks: AtomicU64::new(0),
        zero_lines_read: AtomicU64::new(0),
        sealing: AtomicU64::new(NO_SEAL),
        lanes: (0..=sizing.walk_slots).map(|_lane| RewriteLane::new()).collect(),
        gave_up: Mutex::new(GaveUp::new()),
        early_bar_rewrites: AtomicU64::new(0),
        plain_bar_rewrites: AtomicU64::new(0),
        key_order_copies: AtomicU64::new(0),
        offset_order_copies: AtomicU64::new(0),
        span_reads: AtomicU64::new(0),
        span_records: AtomicU64::new(0),
        span_gap_bytes: AtomicU64::new(0),
        lone_copies: AtomicU64::new(0),
        keyed_copies: AtomicU64::new(0),
        footer_reads: Default::default(),
        span_runs: Default::default(),
        summary_saves: AtomicU64::new(0),
        order: WriteOrder::new(Arc::clone(places)),
        rows_in_index: AtomicU64::new(0),
        seal_steps: SealStepTimes::default(),
        ration: Ration::default(),
        marked_seals: AtomicU64::new(0),
        marked_seal_bytes: AtomicU64::new(0),
        marks_dropped: AtomicU64::new(0),
        is_spinning: is_spinning(io, volume),
        is_device_set: volume.device != DeviceClass::Detect,
        seal_point: AtomicU64::new(volume_rows(config, index, sizing.segment_rows).0),
        seal_rate: AtomicU64::new(0),
        ingest_rate: AtomicU64::new(0),
        seal_started_at: AtomicU64::new(0),
        is_sync_due: AtomicBool::new(false),
        bytes_since_sync: AtomicU64::new(0),
        idle_seals: AtomicU64::new(0),
        unlanded_bytes: AtomicU64::new(0),
        scans_use_cache: AtomicBool::new(false),
        scans_weighed_at: AtomicU64::new(0),
        rooms_given_back: AtomicU64::new(0),
        seal_full_waits: AtomicU64::new(0),
        progress: Arc::clone(progress),
        park_refusals: AtomicU64::new(0),
        overlap_beside: AtomicU64::new(0),
        overlap_output_inputs: AtomicU64::new(0),
        overlap_inputs: AtomicU64::new(0),
        most_in_flight: AtomicU64::new(0),
        callers_seen: AtomicU64::new(0),
        last_call_at: AtomicU64::new(0),
        capped_picks: AtomicU64::new(0),
        covers_held_by_cues: AtomicU64::new(0),
        settle_retries: AtomicU64::new(0),
        pace_refusals: AtomicU64::new(0),
        trust_fallbacks: AtomicU64::new(0),
        dead_counted: AtomicU64::new(0),
        paced_written: AtomicU64::new(0),
        pace_spilled: AtomicU64::new(0),
        outputs_promised: AtomicU64::new(0),
        is_room_refused: AtomicBool::new(false),
        room_refused_at: AtomicU64::new(0),
        promises_outgrown: AtomicU64::new(0),
        parks_promised: AtomicUsize::new(0),
        long_park_waits: AtomicU64::new(0),
        given_up_by_phase: Default::default(),
        given_up_by_kind: Default::default(),
        last_write_at: AtomicU64::new(0),
        seal_interval: AtomicU64::new(0),
        merge_interval: AtomicU64::new(0),
        is_over_bound: AtomicBool::new(true),
        overlap_changes: AtomicU64::new(0),
        walk_slots: (0..sizing.walk_slots).map(|_slot| WalkSlot::new()).collect(),
        // the probe writes a file in each root, and a reader frees nothing
        can_free_blocks: !sizing.is_read_only && volume.roots.iter().all(|root| io.can_free_blocks(root)),
        bytes_freed: AtomicU64::new(0),
        has_held_frees: AtomicBool::new(false),
        seals_done: AtomicU64::new(0),
        walks_finished: AtomicU64::new(0),
        segments_unlinked: AtomicU64::new(0),
        segments_parked: AtomicU64::new(0),
        segments_leaving: AtomicU64::new(0),
        empties_left: AtomicU64::new(0),
        parked: Mutex::new((0..MOST_PARKED * (sizing.walk_slots + 1)).map(|_slot| None).collect()),
        segments_rewritten: AtomicU64::new(0),
        stray_outputs: AtomicU64::new(0),
        reclaim_retries: AtomicU64::new(0),
        has_walk_target: AtomicBool::new(false),
        walk_segment: AtomicU64::new(NO_SEAL),
        is_applying: AtomicBool::new(false),
        claim_lock: Mutex::new(()),
        claims_changed: AtomicU64::new(0),
        looks: LookPass::default(),
        load: LoadCell::default(),
        pace: PaceCell::default(),
        pace_per_mille: AtomicU64::new(PACE_PER_MILLE),
        space_target_per_mille: AtomicU64::new(SPACE_TARGET_PER_MILLE),
        gate_in_flight: AtomicU64::new(0),
        gate_refusals: AtomicU64::new(0),
        asks_heard: AtomicU64::new(0),
        walks_held: AtomicU64::new(0),
        walk_claim_waits: AtomicU64::new(0),
        walk_claim_held: AtomicU64::new(0),
        walk_claim_longest: AtomicU64::new(0),
        held_drain_blocks: AtomicU64::new(0),
        held_drain_bytes: AtomicU64::new(0),
        reserve: ByteCount(volume.segment_bytes.unwrap_or(sizing.segment_bytes).0 * 2),
        summary: Mutex::new(summary.rows),
        open_place,
        open_segment,
        bits_path: summary.paths.root.join(DEAD_BITS_NAME),
        new_bits_path: summary.paths.root.join(NEW_DEAD_BITS_NAME),
        bits_save: Mutex::new(BitsSave { heads: touched_room(SAVE_HEADS_PAGE), chunk: touched_capacity(CHUNK_WORDS * size_of::<u64>(), 0) }),
        bits_saved_at: AtomicU64::new(0),
        summary_paths: summary.paths,
        lost_rows: Mutex::new(summary.lost),
        summary_buffer: Pool::new(vec![touched_buffer(summary_room)]),
        damage: DamageList::new(),
        covers: RangeCovers::new(config.widest_key(index)),
        damaged: Mutex::new(touched_room(MOST_DAMAGED_SPANS)),
        is_clearing: AtomicBool::new(false),
    }
}


/// Work out a volume's row limit: the budget's, or less when its seal's list can't hold that many rows at the volume's widest key,
/// or when a complete column on it needs its row blocks addressable. Wide keys cost rows: a segment of 1,056 byte keys seals at
/// about 850,000 rows whatever the budget
pub fn volume_rows(config: &StoreConfig, volume: usize, rows: RowCount) -> RowCount {
    let listed = rows_within_blocks(seal_blocks(config, volume), config.widest_key(volume));
    let on_volume = config.columns.iter().filter(|column| column.volume.0 as usize == volume && column.complete_rows().is_some());
    let widest = on_volume.map(|column| column.key_shape.widest()).max();
    let addressable = widest.map_or(rows.0, |widest| complete_row_limit(widest).0);
    RowCount(rows.0.min(listed).min(addressable))
}

/// Allocate and touch each complete column's table
pub fn complete_tables(config: &StoreConfig) -> Vec<Option<CompleteTable>> {
    let mut tables: Vec<Option<CompleteTable>> = (0..MAX_COLUMNS).map(|_column| None).collect();
    for column in &config.columns {
        tables[column.id.0 as usize] = column.complete_rows().map(CompleteTable::new);
    }
    tables
}

/// Work out the block lines a volume's complete columns can fill in one table shard: a fair share twice over, for segments
/// that land unevenly, and room for the one segment that lands last
pub fn complete_shard_lines(config: &StoreConfig, volume: usize) -> usize {
    let on_volume = config.columns.iter().filter(|column| column.volume.0 as usize == volume);
    let lines: u64 = on_volume.map(column_lines).sum();
    if lines == 0 {
        return 0;
    }
    (lines as usize).div_ceil(TABLE_SHARDS) * 2 + MOST_SEGMENT_BLOCKS.0 as usize
}

/// Work out the block lines a store sets aside for its complete columns
pub fn complete_lines(config: &StoreConfig) -> u64 {
    let shards: usize = (0..config.volumes.len()).map(|volume| complete_shard_lines(config, volume)).sum();
    (shards * TABLE_SHARDS) as u64
}

/// Work out the block lines a complete column's rows can fill
pub fn column_lines(column: &ColumnConfig) -> u64 {
    let rows_per_block = complete_block_rows(column.key_shape.widest()) as u64;
    column.complete_rows().map_or(0, |rows| rows.0.div_ceil(rows_per_block))
}

/// What open gave a sorted index: the bytes its rule asked for, the bytes it got, and the rows those hold
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SortedSize {
    pub asked: ByteCount,
    pub got: ByteCount,
    pub rows: RowCount,
    /// Value pages its leaves keep values in: its share of the held values by the rows it holds
    pub value_pages: u32,
}

/// Size each sorted column's index from the keys it held at open: its rows on disk, every row counted, since a dead row counted
/// costs room and a live row missed costs keys. The indexes share the spot, block and record caches past their floor, and the
/// caches give up what the indexes take
pub fn size_sorted_indexes(config: &StoreConfig, sizing: &EngineSizing, summaries: &[SummaryFound]) -> (EngineSizing, Vec<Option<SortedSize>>) {
    let mut rows_on_disk = [0_u64; MAX_COLUMNS];
    for summary in summaries {
        for row in summary.rows.rows() {
            let mut cursor = row.cursor();
            for _column in 0..row.column_count {
                let Some(column) = cursor.column() else {
                    break;
                };
                rows_on_disk[column.column.0 as usize] += column.row_count.0;
            }
        }
    }
    let sorted: Vec<&ColumnConfig> = config.columns.iter().filter(|column| column.is_sorted()).collect();
    let (spot, block, record) = (sizing.spot_share.0, sizing.cache_share.0, sizing.record_share.0);
    // where every column is sorted no get reads the spot cache, so its share is the indexes' first, and the floor holds on the
    // block and record caches alone
    let is_all_sorted = !config.columns.is_empty() && config.columns.iter().all(|column| column.is_sorted());
    let cap = if is_all_sorted { spot + index_cap(block + record) } else { index_cap(spot + block + record) };
    // the store keeps one headroom for all its indexes, a quarter of the cap shared evenly
    let headroom = index_headroom(cap, sorted.len());
    let asks: Vec<IndexAsk> = sorted
        .iter()
        .map(|column| {
            let width = column.key_shape.widest();
            // a column with no least rows leaves the store to size the index alone
            let least = column.sorted_least();
            if is_index_exact() {
                return IndexAsk::Bytes(planned_index_bytes(width, least.unwrap_or(RowCount(0))));
            }
            index_ask(width, rows_on_disk[column.id.0 as usize], least, headroom)
        })
        .collect();
    // an exact index asks for what it declared, and past the cap it gets its share as any index does
    let granted = share_indexes(&asks, cap);
    let mut sizes: Vec<Option<SortedSize>> = (0..MAX_COLUMNS).map(|_column| None).collect();
    let part = cap / asks.len().max(1) as u64;
    let mut taken = 0;
    for ((column, ask), got) in sorted.iter().zip(&asks).zip(&granted) {
        let asked = ByteCount(if let IndexAsk::Bytes(bytes) = ask { bytes.0 } else { part });
        let width = column.key_shape.widest();
        let declared = column.sorted_least().unwrap_or(RowCount(0));
        let is_whole = is_index_exact() && *got >= asked.0;
        // a grant under one pool's floor, a page for each part, builds no index, and the column reads its footers
        let rows = if is_whole { declared } else if *got < planned_index_bytes(width, RowCount(1)).0 { RowCount(0) } else { rows_for_bytes(width, ByteCount(*got)) };
        // the caches give up what the pool holds, which a grant rounds down to
        let held = if rows.0 == 0 { 0 } else { planned_index_bytes(width, rows).0 };
        taken += held;
        sizes[column.id.0 as usize] = Some(SortedSize { asked, got: ByteCount(held), rows, value_pages: 0 });
    }
    // in a store of sorted columns the spot share pays first, and what it has left goes to the block and record caches
    let (from_caches, to_caches) = if is_all_sorted { (taken.saturating_sub(spot), spot.saturating_sub(taken)) } else { (taken, 0) };
    // each cache gives and takes by its size
    let caches = if is_all_sorted { block + record } else { spot + block + record };
    let by_size = |share: u64, bytes: u64| (u128::from(share) * u128::from(bytes) / u128::from(caches.max(1))) as u64;
    let changed = |share: u64| (share + by_size(share, to_caches)).saturating_sub(by_size(share, from_caches));
    // the held values go to the sorted columns by the rows each holds, and to the record cache where no column holds any
    let all_rows: u64 = sizes.iter().flatten().map(|size| size.rows.0).sum();
    for size in sizes.iter_mut().flatten() {
        let bytes = u128::from(sizing.value_share.0) * u128::from(size.rows.0) / u128::from(all_rows.max(1));
        size.value_pages = (bytes / VALUE_PAGE_BYTES as u128).min(u128::from(u32::MAX - 1)) as u32;
    }
    let unheld = if all_rows == 0 { sizing.value_share.0 } else { 0 };
    let shared = EngineSizing {
        spot_share: ByteCount(if is_all_sorted { 0 } else { changed(spot) }),
        cache_share: ByteCount(changed(block)),
        record_share: ByteCount(changed(record) + unheld),
        ..*sizing
    };
    (shared, sizes)
}

/// Allocate and touch each sorted column's index at the rows open sized it for
pub fn sorted_indexes(config: &StoreConfig, sizes: &[Option<SortedSize>]) -> Vec<Option<SortedKeys>> {
    let mut indexes: Vec<Option<SortedKeys>> = (0..MAX_COLUMNS).map(|_column| None).collect();
    // every index's share of value pages goes in one pool, so the columns walks read take what the others leave
    let pages: u64 = sizes.iter().flatten().filter(|size| size.rows.0 > 0).map(|size| u64::from(size.value_pages)).sum();
    let pool = Arc::new(ValuePages::new(u32::try_from(pages).unwrap_or(u32::MAX - 1)));
    let mut has_owner = false;
    for column in &config.columns {
        indexes[column.id.0 as usize] = sizes[column.id.0 as usize].filter(|size| size.rows.0 > 0).map(|size| {
            let index = SortedKeys::new(column.key_shape.widest(), size.rows).holding_values(Arc::clone(&pool), !has_owner);
            has_owner = true;
            index
        });
    }
    indexes
}

/// Name each complete column's image files and allocate its buffer
pub fn complete_images(config: &StoreConfig) -> Vec<Option<Mutex<ImageFiles>>> {
    let mut images: Vec<Option<Mutex<ImageFiles>>> = (0..MAX_COLUMNS).map(|_column| None).collect();
    for column in config.columns.iter().filter(|column| column.complete_rows().is_some()) {
        if let Some(root) = config.volumes[column.volume.0 as usize].roots.first() {
            images[column.id.0 as usize] = Some(Mutex::new(ImageFiles::new(root, column.id)));
        }
    }
    images
}

/// Work out what a volume sits on: what the config says, or what the kernel says of its first root. Unknown counts as solid
pub fn is_spinning<Backend: Io>(io: &Backend, volume: &VolumeConfig) -> bool {
    match volume.device {
        DeviceClass::Spinning => true,
        DeviceClass::Solid => false,
        DeviceClass::Detect => volume.roots.first().and_then(|root| io.is_rotational(root)).unwrap_or(false),
    }
}

/// Read the mean record the summaries hold, key and header included, or nothing for a store with no rows on disk
pub fn mean_record(summaries: &[SummaryFound]) -> Option<u64> {
    let (mut bytes, mut rows) = (0, 0);
    for summary in summaries {
        for row in summary.rows.rows() {
            let (row_bytes, row_rows) = record_totals(&row);
            bytes += row_bytes;
            rows += row_rows;
        }
    }
    (rows > 0).then(|| bytes / rows)
}

/// Read the mean record over the columns whose records the record cache can take, weighted by rows, so a column of values too
/// wide to cache leaves the cache's plan alone. Nothing when no such column has rows on disk
pub fn cacheable_mean(summaries: &[SummaryFound]) -> Option<u64> {
    let mut columns = [(0_u64, 0_u64); MAX_COLUMNS];
    for summary in summaries {
        for row in summary.rows.rows() {
            let mut cursor = row.cursor();
            for _column in 0..row.column_count {
                let Some(column) = cursor.column() else {
                    break;
                };
                let (bytes, rows) = &mut columns[column.column.0 as usize];
                *bytes += column.live_bytes.0 + column.dead_bytes.0;
                *rows += column.row_count.0;
            }
        }
    }
    let held = columns.iter().filter(|(bytes, rows)| *rows > 0 && bytes / rows <= MOST_CACHED_BYTES as u64);
    let (bytes, rows) = held.fold((0, 0), |(bytes, rows), (column_bytes, column_rows)| (bytes + column_bytes, rows + column_rows));
    (rows > 0).then(|| bytes / rows)
}

/// Read the least budget a config takes as a writer or a reader, as a refusal at a budget too small for anything would give it:
/// the shape with no ring, the workers the config sets or none, and LEAST_LZ4_BUDGET past it for a store with an lz4 column
pub fn least_budget_of<Backend: Io>(io: &Backend, config: &StoreConfig, is_read_only: bool) -> Result<ByteCount, StoreError> {
    // the config is checked for all but its budget, which is what the call answers
    check_config(&StoreConfig { memory_budget: ByteCount(u64::MAX), ..config.clone() })?;
    // a root not made yet sits on the disk of its nearest ancestor that exists
    let mut placed = config.clone();
    for root in placed.volumes.iter_mut().flat_map(|volume| volume.roots.iter_mut()) {
        *root = disk_root(io, root);
    }
    let disks = volume_disks(io, &placed).map_err(open_error("reading a disk's size"))?;
    let shape = StoreShape { budget: ByteCount(0), is_read_only, ..store_shape(config, false, &disks) };
    // an lz4 column also needs its compress pool, which no budget under LEAST_LZ4_BUDGET holds
    let has_lz4 = config.columns.iter().any(|column| column.codec == Codec::Lz4);
    let lz4_least = if has_lz4 { LEAST_LZ4_BUDGET } else { ByteCount(0) };
    Ok(shape.with_workers_settled().least_budget().max(lz4_least))
}

/// Read the path whose disk a root sits on: the root, or its nearest ancestor that exists when the root isn't made yet. The
/// empty ancestor of a relative path is the current directory
pub fn disk_root<Backend: Io>(io: &Backend, root: &Path) -> PathBuf {
    root.ancestors()
        .map(|ancestor| if ancestor.as_os_str().is_empty() { Path::new(".") } else { ancestor })
        .find(|ancestor| io.capacity(ancestor).is_ok())
        .map_or_else(|| root.to_path_buf(), Path::to_path_buf)
}

/// Check every block of a segment with exact bits has as many bits set as the summary counts dead rows, so a row charged
/// twice or with no bit shows at its cause. The sims run it at every open and close
#[cfg(feature = "test-hooks")]
pub fn check_dead_counts<Backend: Io>(state: &EngineState<Backend>) {
    for volume in &state.volumes {
        let rows = hold(&volume.summary);
        for row in rows.rows() {
            let segment = row.facts.segment;
            if volume.dead_bits.exact(segment).is_none() {
                continue;
            }
            for (block, dead) in row.block_dead.chunks_exact(size_of::<u32>()).enumerate() {
                let dead = u32::from_le_bytes(dead.try_into().unwrap_or_default());
                let set = volume.dead_bits.block_set(segment, block as u32);
                assert_eq!(set, Some(dead), "{segment:?} block {block} counts {dead} dead rows and has {set:?} bits set");
            }
        }
    }
}


/// Blocks the bit check has read, so a sim can show the check ran
#[cfg(feature = "test-hooks")]
pub static BLOCKS_PROVEN: AtomicU64 = AtomicU64::new(0);

/// Check every set bit of a segment with exact bits is true: its row has a newer version in a sealed segment, lies under an
/// applied cover or a damaged range, is a cover's own row, or is a grave with nothing older left. A bit on a live row would let
/// a merge drop it. The buffers sit on the stack, so a close that runs it allocates nothing
#[cfg(feature = "test-hooks")]
pub fn check_dead_bits_true<Backend: Io>(state: &EngineState<Backend>) {
    use crate::engine::complete_path::KeyBuffer;
    use crate::io::{InPlace, IoFile, in_place};
    use crate::row_kind::RowKind;
    use crate::segment::row_block::{ROW_BLOCK_BYTES, RowBlockWalk};

    const SEGMENTS_A_LOOK: usize = 256;
    let mut bytes = [0_u8; 2 * ROW_BLOCK_BYTES];
    let mut key = KeyBuffer::new();
    for volume in &state.volumes {
        let mut first = 0;
        loop {
            let mut segments = [SegmentNumber(0); SEGMENTS_A_LOOK];
            let mut count = 0;
            for (slot, row) in segments.iter_mut().zip(hold(&volume.summary).rows().skip(first)) {
                *slot = row.facts.segment;
                count += 1;
            }
            if count == 0 {
                break;
            }
            first += count;
            for segment in segments[..count].iter().copied() {
                let (Some(extent), Some(pin)) = (volume.dead_bits.exact(segment), volume.table.pin(segment)) else {
                    continue;
                };
                let mut index = 0;
                while let Some(block) = volume.table.walk_block(segment, index) {
                    let length = block.entry.block_length().0 as usize;
                    if length > bytes.len() || pin.file.read_exact_at(block.entry.block_at(), &mut bytes[..length]).is_err() {
                        break;
                    }
                    let Some(mut walk) = RowBlockWalk::open(&bytes[..length], pin.head.place_base) else {
                        break;
                    };
                    BLOCKS_PROVEN.fetch_add(1, Ordering::Relaxed);
                    while let Some(row) = walk.next_row(&mut key) {
                        let at = walk.row_index() as u32;
                        if volume.dead_bits.is_set(&extent, index, at) != Some(true) || row.kind == RowKind::RangeDelete {
                            continue;
                        }
                        let Ok(read) = state.key_read(block.column, &key, WritePlace(u64::MAX), false) else {
                            continue;
                        };
                        let Ok(newest) = in_place(state.search_footers(&InPlace, volume, &read)) else {
                            continue;
                        };
                        let is_replaced = newest.is_some_and(|found| found.row.place > row.place);
                        // a cover reloaded at open is durable, and a get reads through it before it applies again
                        let is_covered = volume.covers.newest_over(block.column, &key, WritePlace(u64::MAX)).is_some_and(|cover| cover > row.place);
                        let is_hidden = volume.damage.covering(block.column, &key).is_some_and(|highest| highest > row.place);
                        let is_bare_grave = row.kind == RowKind::Grave && !state.has_older_row(volume, block.column, &key, row.place, &[]).unwrap_or(true);
                        assert!(is_replaced || is_covered || is_hidden || is_bare_grave, "{segment:?} block {index} row {at} has its bit set and is live: key {:02x?}", &key[..]);
                    }
                    index += 1;
                }
            }
        }
    }
}
