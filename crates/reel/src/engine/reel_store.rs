//! A store: open, the calls consumers make, and close

use std::io::{Error, ErrorKind};
use std::mem;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use reel_api::column_id::ColumnId;
use reel_api::memory_report::MemoryReport;
use reel_api::reads::ValueWindow;
use reel_api::store_error::StoreError;
use reel_api::value_list::ValueList;
use reel_api::units::{ByteCount, Nanoseconds, VolumeId, WritePlace};
use reel_api::write_batch::WriteBatch;

use crate::config::{ColumnConfig, StoreConfig};
use crate::engine::column_info::ColumnInfo;
use crate::engine::engine_sizing::{
    EngineSizing, FILTER_CACHE_QUARTERS, FILTER_SLOT_BYTES, GROUP_BYTES, LEAST_LZ4_BUDGET, PLANNED_COLUMN_BYTES, PLANNED_LINE_BYTES, SHARED_JOBS,
    PLANNED_SEGMENT_BYTES, StoreShape, GATHER_BYTES, READ_BUFFERS, WRITE_BUFFERS,
};
use crate::engine::engine_state::{EngineState, PLACE_RUN, SEGMENT_NUMBER_BATCH, SlotFile};
use crate::engine::open_segment::{RECORD_WINDOW_BYTES, RecordWindow, RowQueue, fits_a_segment_path};
use crate::engine::read_path::RecordWant;
use crate::engine::recovery::{
    RootScan, SummaryFound, list_roots, load_covers, read_summaries, recover_segments, save_cleared_spans, scan_roots,
    walk_open_rows,
    settle_summaries,
};
use crate::engine::reserve_job::ReserveJob;
use crate::engine::row_job::RowJob;
use crate::engine::seal_job::SealJob;
use crate::engine::compact_job::CompactJob;
use crate::engine::cue::{Cue, CueTable};
use crate::engine::planner::Planner;
use crate::engine::walk_job::{BlockCounter, WalkJob, WalkSlot, WalkWorker};
use crate::engine::volume::{ClaimState, MOST_PARKED, NO_SEAL, SyncState, Volume};
use crate::guard::hold;
use crate::index::open_slots::OpenSlot;
use crate::index::spot_index::SpotIndex;
use crate::index::write_index::WriteIndex;
use crate::io::lock_outcome::LockOutcome;
use crate::io::{Io, IoTask};
use crate::pool::{Pool, touched_buffer, touched_filled, touched_room, touched_zeroes};
use crate::record::codec::Codec;
use crate::record::record_check::RecordKey;
use crate::record::record_encoder::{RecordEncoder, SHARE_BYTES};
use crate::record::record_reader::RecordReader;
use crate::rows::row_group::RowGroupBuffer;
use crate::segment::block_cache::BlockCache;
use crate::segment::column_directory::ColumnDirectory;
use crate::segment::damage_list::{DamageList, MOST_DAMAGED_SPANS, SPAN_KEY_BYTES};
use crate::segment::range_covers::RangeCovers;
use crate::segment::summary::{MOST_ROW_COLUMN_BYTES, MOST_ROW_FIXED_BYTES, MOST_ROW_SPAN_FIXED_BYTES};
use crate::segment::footer_reader::FooterReader;
use crate::segment::row_block::ROW_BLOCK_BYTES;
use crate::segment::segment_table::{SegmentTable, TABLE_SHARDS, TableSizing};
use crate::segment::segment_tail::{MAX_COLUMNS, SegmentNumber};
use crate::superblock::{RootCopies, StoreId, StoredColumn, StoredVolume, Superblock, SuperblockPaths, stored_column};
use crate::write_order::WriteOrder;

/// The lock file in each root
pub const LOCK_NAME: &str = "lock";

/// An open store
pub struct ReelStore<Backend: Io> {
    pub state: Arc<EngineState<Backend>>,
    is_closed: AtomicBool,
}

/// Turn a backend failure at open into a store error
pub fn open_error(action: &'static str) -> impl Fn(Error) -> StoreError {
    move |source| StoreError::Io { action, source }
}

impl<Backend: Io> ReelStore<Backend> {
    /// Open a store: lock every root, read every tail, recover every open segment, start the jobs
    pub fn open(io: Backend, config: &StoreConfig) -> Result<Self, StoreError> {
        check_config(config)?;
        let locks = lock_roots(&io, config)?;
        let superblock = settle_superblock(&io, config)?;

        let mut root_count = 0;
        for volume in &config.volumes {
            root_count += volume.roots.len();
        }
        let shape = StoreShape {
            budget: config.memory_budget,
            roots: root_count,
            volumes: config.volumes.len(),
            has_lz4: config.columns.iter().any(|column| column.codec == Codec::Lz4),
            widest_keys: (0..config.volumes.len()).map(|volume| config.widest_key(volume) as u64).sum(),
        };
        if config.memory_budget < shape.least_budget() {
            return Err(StoreError::BudgetTooSmall {
                budget: config.memory_budget,
                least: shape.least_budget(),
            });
        }
        let sizing = EngineSizing::for_store(&shape);

        // every root is listed before any tail is read, and the table is sized from what they hold
        let mut roots = Vec::with_capacity(config.volumes.len());
        for (index, volume) in config.volumes.iter().enumerate() {
            roots.push((index, volume.roots.clone()));
        }
        let listings = list_roots(&io, &roots).map_err(open_error("listing a root"))?;
        let record_key = superblock.record_key;
        let mut scan = scan_roots(&io, record_key, &listings).map_err(open_error("reading a tail"))?;
        let summaries = match read_summaries(&io, &roots) {
            Ok(summaries) => summaries,
            Err(error) => {
                scan.close_files(&io);
                return Err(open_error("reading a summary")(error));
            },
        };

        let state = Arc::new(build_state(io, config, &sizing, superblock, locks, &scan, summaries));
        if let Err(error) = bring_up(&state, &mut scan) {
            // a caller can retry, so a failed open leaves no handle behind
            let _stopped = state.stop_jobs();
            scan.close_files(&state.io);
            for volume in &state.volumes {
                state.close_volume(volume);
            }
            return Err(error);
        }

        Ok(Self {
            state,
            is_closed: AtomicBool::new(false),
        })
    }

    /// Count the summary rows that didn't fit, each one a segment whose damage would cover its columns whole
    pub fn dropped_summary_rows(&self) -> u64 {
        self.state.dropped_summary_rows.load(Ordering::Acquire)
    }

    /// Report the heap each structure holds
    pub fn memory_report(&self) -> MemoryReport {
        self.state.memory_report()
    }

    /// Count the segments open kept and couldn't read
    pub fn damaged_segments(&self) -> u64 {
        self.state.damaged_segments.load(Ordering::Acquire)
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
        self.check_open()?;
        self.state.write_value(column, key, value)
    }

    /// Count a volume's pending range deletes
    pub fn pending_range_deletes(&self, volume: VolumeId) -> usize {
        self.state.volumes[volume.0 as usize].covers.len()
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
        self.check_open()?;
        self.state.write_batch(batch)
    }

    /// Write a value and take its buffer, which a none column sends to the file as it stands
    pub fn put_owned(&self, column: ColumnId, key: &[u8], value: Vec<u8>) -> Result<(), StoreError> {
        self.put(column, key, &value)
    }

    /// Remove every key in [start, end) written before this call, as one record whatever the keys it covers
    pub fn delete_range(&self, column: ColumnId, start: &[u8], end: &[u8]) -> Result<(), StoreError> {
        self.check_open()?;
        self.state.write_range_delete(column, start, end)
    }

    /// Write a grave
    pub fn delete(&self, column: ColumnId, key: &[u8]) -> Result<(), StoreError> {
        self.check_open()?;
        self.state.write_grave(column, key)
    }

    /// Fill a buffer with the newest value, and say whether the key has one
    pub fn get(&self, column: ColumnId, key: &[u8], buf: &mut Vec<u8>) -> Result<bool, StoreError> {
        self.check_open()?;
        self.state.get_latest(column, key, RecordWant::Whole, buf)
    }

    /// Answer every key at one point in the write order, with a bad record as that key's answer
    pub fn get_many(&self, column: ColumnId, keys: &[&[u8]], values: &mut ValueList) -> Result<(), StoreError> {
        self.check_open()?;
        self.state.get_many_now(column, keys, values)
    }

    /// Fill a buffer with a checked window of the value, and say whether the key has one
    pub fn get_range(
        &self,
        column: ColumnId,
        key: &[u8],
        window: ValueWindow,
        buf: &mut Vec<u8>,
    ) -> Result<bool, StoreError> {
        self.check_open()?;
        let want = RecordWant::Window(window);
        self.state.get_latest(column, key, want, buf)
    }

    /// Take a read view at this point in the write order, which copies no data and never blocks a writer
    pub fn cue(&self) -> Result<Cue<Backend>, StoreError> {
        self.check_open()?;
        Cue::new(Arc::clone(&self.state))
    }

    /// Check whether the key has a value, without reading the value
    pub fn contains(&self, column: ColumnId, key: &[u8]) -> Result<bool, StoreError> {
        self.check_open()?;
        self.state.contains_now(column, key)
    }

    /// Clear a damaged span once the caller has repaired it from peers, up to an end key or the column's end
    pub fn clear_damage(&self, column: ColumnId, start: &[u8], end: Option<&[u8]>) -> Result<(), StoreError> {
        self.check_open()?;
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
    pub fn clear_spans(
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
        self.check_open()?;
        for volume in &self.state.volumes {
            self.state.sync_volume(volume)?;
        }
        Ok(())
    }

    /// Mark each volume's fullest taking segment to seal, as an idle volume does on its own
    pub fn seal_now(&self) {
        for volume in &self.state.volumes {
            self.state.seal_fullest(volume);
        }
    }

    /// Stop every job, make every returned write durable and release the locks
    pub fn close(&self) -> Result<(), StoreError> {
        if self.is_closed.swap(true, Ordering::AcqRel) {
            return Ok(());
        }

        self.state.stop_jobs().map_err(open_error("stopping a job"))?;

        let mut outcome = Ok(());
        for volume in &self.state.volumes {
            if !volume.has_failed() {
                outcome = outcome.and(self.state.sync_volume(volume));
            }
            if !volume.has_failed() {
                outcome = outcome.and(self.state.save_summary(volume).map_err(open_error("saving a summary")));
            }
            // writes that returned on a failed volume may not be durable, and the caller hears it
            if volume.has_failed() {
                outcome = outcome.and(Err(StoreError::VolumeFailed { volume: volume.id }));
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
    pub fn stop_jobs(&self) -> Result<(), Error> {
        self.is_stopping.store(true, Ordering::Release);
        if let Some(task) = self.row_task.get() {
            task.stop()?;
        }
        if let Some(task) = self.reserve_task.get() {
            task.stop()?;
        }
        for slot in self.volumes.iter().flat_map(|volume| &volume.walk_slots) {
            if let Some(task) = slot.task.get() {
                task.stop()?;
            }
        }
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
    pub fn close_volume(&self, volume: &Volume<Backend>) {
        // a read that outlives its store keeps its handle, and open finishes what the file's leaving started
        for slot in hold(&volume.parked).iter_mut() {
            if slot.as_ref().is_some_and(|parked| volume.table.retired_pins(&parked.retired) == 0)
                && let Some(parked) = slot.take()
            {
                // a file that stays is one open walks dead again, and a close has no later step to try
                if let Err(parked) = self.clear_parked(volume, parked) {
                    self.io.close_file(parked.retired.file);
                }
            }
        }
        // a stopped rewrite leaves the store as it found it
        let [data, graves] = mem::take(&mut *hold(&volume.partial_outputs));
        let mut is_data_gone = true;
        if let Some(Err(output)) = data.map(|output| self.remove_output(volume, output)) {
            is_data_gone = false;
            self.io.close_file(output.file);
        }
        // a data output left on disk needs its graves at the next open
        if let Some(output) = graves {
            let left = if is_data_gone { self.remove_output(volume, output) } else { Err(output) };
            if let Err(output) = left {
                self.io.close_file(output.file);
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
            let mut guard = volume.table.write_shard(shard);
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
    // damage ranks at the counter, so the counter passes every place open can find first
    for found in &scan.sealed {
        state.order.resume_past(found.head.places.highest);
    }
    walk_open_rows(state, scan).map_err(open_error("walking a rows file"))?;
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
    let damaged: usize = state.volumes.iter().map(|volume| hold(&volume.damaged).len()).sum();
    state.damaged_segments.store(damaged as u64, Ordering::Release);

    // some platforms set a lock up at its first use, and close is the first to take this one
    for volume in &state.volumes {
        drop(hold(&volume.locks));
    }

    // an open is not a state: the limits say what a run-through would seal, from the same rows
    for volume in &state.volumes {
        let mut claims = hold(&volume.claims);
        state.mark_full_segments(&mut claims, volume);
    }

    let row_job = RowJob {
        state: Arc::clone(state),
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
        state.seal_heap.fetch_add(walk.heap_bytes().0, Ordering::AcqRel);
        walks.push(walk);
        let compaction = CompactJob::new(Arc::clone(state), volume);
        state.compaction_heap.fetch_add(compaction.heap_bytes().0, Ordering::AcqRel);
        compactions.push(compaction);
    }
    let mut workers = 0;
    for (volume, held) in state.volumes.iter().enumerate() {
        for (index, slot) in held.walk_slots.iter().enumerate() {
            let worker = WalkWorker {
                state: Arc::clone(state),
                volume,
                slot: index,
                counter: BlockCounter::new(Arc::clone(state)),
            };
            state.seal_heap.fetch_add(worker.counter.heap_bytes().0 + slot.heap_bytes().0, Ordering::AcqRel);
            let task = state.io.spawn_job("reel-walk", worker).map_err(open_error("starting a job"))?;
            let _set = slot.task.set(task);
            workers += 1;
        }
    }
    let planner = Planner {
        state: Arc::clone(state),
        walks,
        compactions,
        last: 0,
    };
    let task = state.io.spawn_job("reel-planner", planner).map_err(open_error("starting a job"))?;
    let _set = state.planner_task.set(task);
    let reserve_job = ReserveJob {
        state: Arc::clone(state),
    };
    let task = state.io.spawn_job("reel-reserve", reserve_job).map_err(open_error("starting a job"))?;
    let _set = state.reserve_task.set(task);
    state.job_count.store(SHARED_JOBS + state.volumes.len() as u64 + workers, Ordering::Release);
    Ok(())
}

/// Refuse a config that can't work
pub fn check_config(config: &StoreConfig) -> Result<(), StoreError> {
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
        if seen[column.id.0 as usize] || !column.key_shape.is_valid() {
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
pub fn settle_superblock<Backend: Io>(io: &Backend, config: &StoreConfig) -> Result<Superblock, StoreError> {
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

    // no copy anywhere that passes, with some that exist, is a lost store and never a new one
    if newest.is_none() && has_failed {
        return Err(StoreError::Io {
            action: "reading a superblock",
            source: Error::from(ErrorKind::InvalidData),
        });
    }

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
    for stored in &superblock.volumes {
        let Some(volume) = config.volumes.iter().find(|volume| volume.name == stored.name) else {
            return Err(StoreError::Io {
                action: "checking the volume layout",
                source: Error::other("config dropped a volume"),
            });
        };
        for root in &stored.roots {
            let is_declared = volume.roots.iter().any(|path| path.to_string_lossy() == root.as_str());
            if !is_declared && !superblock.dead_roots.contains(root) {
                return Err(StoreError::Io {
                    action: "checking the volume layout",
                    source: Error::other("config dropped a root"),
                });
            }
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

/// Allocate and touch everything the engine will hold
pub fn build_state<Backend: Io>(
    io: Backend,
    config: &StoreConfig,
    sizing: &EngineSizing,
    superblock: Superblock,
    mut locks: Vec<Vec<Backend::Lock>>,
    scan: &RootScan<Backend::File>,
    summaries: Vec<SummaryFound>,
) -> EngineState<Backend> {
    // a volume's table and directories come out of its part of the share, by the columns it can hold
    let volume_share = sizing.table_share.0 / config.volumes.len().max(1) as u64;
    let most_columns = (0..config.volumes.len())
        .map(|volume| config.columns.iter().filter(|column| column.volume.0 as usize == volume).count())
        .max()
        .unwrap_or(1)
        .max(1);
    let segment_bytes = PLANNED_SEGMENT_BYTES + most_columns as u64 * PLANNED_COLUMN_BYTES;
    let segments_per_shard = ((volume_share / 2 / segment_bytes) as usize / TABLE_SHARDS).max(8);
    let table_sizing = TableSizing {
        segments_per_shard,
        columns_per_shard: segments_per_shard * most_columns,
        blocks_per_shard: ((volume_share / 2 / PLANNED_LINE_BYTES) as usize / TABLE_SHARDS).max(64),
    };

    let mut columns: Vec<Option<ColumnInfo>> = vec![None; MAX_COLUMNS];
    let mut directories: Vec<Option<ColumnDirectory>> = Vec::with_capacity(MAX_COLUMNS);
    for _column in 0..MAX_COLUMNS {
        directories.push(None);
    }
    let mut has_lz4 = false;
    for column in &config.columns {
        has_lz4 |= column.codec == Codec::Lz4;
        columns[column.id.0 as usize] = Some(ColumnInfo {
            key_shape: column.key_shape,
            codec: column.codec,
            volume: column.volume,
        });
        let found = scan.sealed.len();
        let room = (segments_per_shard * TABLE_SHARDS).max(found * 2);
        directories[column.id.0 as usize] = Some(ColumnDirectory::new(room));
    }

    let open_slots = sizing.shard.open_slots;
    let mut volumes = Vec::with_capacity(config.volumes.len());
    let all_row_room = config.volumes.len() * MOST_ROW_FIXED_BYTES + config.columns.len() * MOST_ROW_COLUMN_BYTES;
    let mut summaries = summaries.into_iter();
    for (index, volume) in config.volumes.iter().enumerate() {
        let mut paths = Vec::with_capacity(volume.roots.len());
        for root in &volume.roots {
            paths.push(SuperblockPaths::new(root));
        }
        let held = mem::take(&mut locks[index]);
        let Some(mut summary) = summaries.next() else {
            continue;
        };
        let found_summary = summary.rows.bytes().len() * 2;
        let columns_here = config.columns.iter().filter(|column| column.volume.0 as usize == index).count();
        // the share splits by each volume's row size, so every volume plans the same number of rows
        // the share pays for the rows and the buffer they save through
        let planned_summary = sizing.summary_share.0 as usize / 2 * row_room(columns_here) / all_row_room;
        // open never drops a row, so a segment's damage keeps its rank through every open
        let span_room = MOST_DAMAGED_SPANS * (MOST_ROW_SPAN_FIXED_BYTES + 2 * SPAN_KEY_BYTES);
        let summary_room = span_room + found_summary.max(planned_summary).max(scan.least_summary_room(index, columns_here) * 2);
        summary.rows.make_room(ByteCount(summary_room as u64));
        let room = table_room(&table_sizing, &scan.least_sizing(index));
        // a row for each segment the table can hold, beside open segments and the spans of gone ones
        summary.rows.make_index(room.segments_per_shard * TABLE_SHARDS + open_slots + MOST_DAMAGED_SPANS);
        volumes.push(Volume {
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
                bytes_since_sync: ByteCount(0),
                next_root: 0,
            }),
            table: SegmentTable::new(&room),
            view: AtomicU64::new(0),
            handovers: AtomicU64::new(0),
            highest_sealed: AtomicU64::new(0),
            sync: Mutex::new(SyncState::default()),
            is_failed: AtomicBool::new(false),
            segment_bytes: sizing.segment_bytes,
            segment_rows: sizing.segment_rows,
            walk_retries: AtomicU64::new(0),
            walks_waiting: AtomicU64::new(0),
            wants_compaction: AtomicBool::new(true),
            has_kept_covers: AtomicBool::new(false),
            is_compaction_held: AtomicBool::new(false),
            is_seal_stuck: AtomicBool::new(false),
            sealing: AtomicU64::new(NO_SEAL),
            walk_slots: (0..sizing.walk_workers).map(|_slot| WalkSlot::new()).collect(),
            can_free_blocks: volume.roots.iter().all(|root| io.can_free_blocks(root)),
            bytes_freed: AtomicU64::new(0),
            has_held_frees: AtomicBool::new(false),
            segments_unlinked: AtomicU64::new(0),
            segments_parked: AtomicU64::new(0),
            parked: Mutex::new([const { None }; MOST_PARKED]),
            segments_rewritten: AtomicU64::new(0),
            is_rewriting: AtomicBool::new(false),
            stray_outputs: AtomicU64::new(0),
            reclaim_retries: AtomicU64::new(0),
            has_walk_target: AtomicBool::new(false),
            walks_held: AtomicU64::new(0),
            partial_outputs: Mutex::new([None, None]),
            reserve: ByteCount(sizing.segment_bytes.0 * 2),
            summary: Mutex::new(summary.rows),
            summary_paths: summary.paths,
            lost_rows: Mutex::new(summary.lost),
            summary_buffer: Pool::new(vec![touched_buffer(summary_room)]),
            damage: DamageList::new(),
            covers: RangeCovers::new(config.widest_key(index)),
            damaged: Mutex::new(touched_room(MOST_DAMAGED_SPANS)),
            is_clearing: AtomicBool::new(false),
        });
    }

    let mut queues = Vec::with_capacity(open_slots);
    let mut spare_groups = Vec::with_capacity(open_slots);
    let mut windows = Vec::with_capacity(open_slots);
    let mut window_copies = Vec::with_capacity(open_slots);
    let mut slot_files = Vec::with_capacity(open_slots);
    let mut free_slots = Vec::with_capacity(open_slots);
    for slot in 0..open_slots {
        queues.push(Mutex::new(RowQueue::new(GROUP_BYTES)));
        spare_groups.push(Mutex::new(Some(RowGroupBuffer::new(GROUP_BYTES))));
        windows.push([RecordWindow::new(), RecordWindow::new()]);
        window_copies.push(Mutex::new(touched_zeroes(RECORD_WINDOW_BYTES)));
        slot_files.push(SlotFile::new());
        free_slots.push(OpenSlot((open_slots - 1 - slot) as u8));
    }

    let largest_value = sizing.segment_bytes;
    let most_shares = if has_lz4 { sizing.lz4_stored_limit.0 as usize / SHARE_BYTES } else { 0 };
    let mut encoders = Vec::with_capacity(WRITE_BUFFERS);
    for _encoder in 0..WRITE_BUFFERS {
        encoders.push(RecordEncoder::new(largest_value, most_shares));
    }
    let shares = (0..most_shares).map(|_share| touched_zeroes(SHARE_BYTES)).collect();

    let mut record_readers = Vec::with_capacity(READ_BUFFERS);
    let mut footer_readers = Vec::with_capacity(READ_BUFFERS);
    for _reader in 0..READ_BUFFERS {
        record_readers.push(RecordReader::new());
        footer_readers.push(FooterReader::new());
    }

    let mut seal_tasks = Vec::with_capacity(config.volumes.len());
    for _volume in &config.volumes {
        seal_tasks.push(OnceLock::new());
    }

    let filter_share = ByteCount(sizing.cache_share.0 / 4 * FILTER_CACHE_QUARTERS);
    let row_share = ByteCount(sizing.cache_share.0 - filter_share.0);
    let reserved = superblock.next_segment.0;
    let first_number = reserved.saturating_sub(SEGMENT_NUMBER_BATCH);
    let place_limit = superblock.place_limit;
    let first_place = place_limit.0.saturating_sub(PLACE_RUN);

    EngineState {
        io,
        record_key: superblock.record_key,
        sizing: *sizing,
        columns,
        directories,
        volumes,
        order: WriteOrder::new(WritePlace(first_place), place_limit),
        index: WriteIndex::new(&sizing.shard),
        spot: SpotIndex::new(sizing.spot_share),
        filter_cache: BlockCache::new(filter_share, FILTER_SLOT_BYTES),
        row_cache: BlockCache::new(row_share, ROW_BLOCK_BYTES),
        queues,
        spare_groups,
        windows,
        window_copies,
        slot_files,
        free_slots: Mutex::new(free_slots),
        encoders: Pool::new(encoders),
        stored_shares: Pool::new(shares),
        batch_rows: Pool::new((0..WRITE_BUFFERS).map(|_buffer| RowGroupBuffer::new(GROUP_BYTES)).collect()),
        batch_records: Pool::new((0..WRITE_BUFFERS).map(|_buffer| touched_zeroes(GATHER_BYTES)).collect()),
        batch_lengths: Pool::new(vec![touched_filled(sizing.segment_rows.0 as usize, 0)]),
        longest_turn: AtomicU64::new(0),
        index_waits: AtomicU64::new(0),
        longest_index_wait: AtomicU64::new(0),
        record_readers: Pool::new(record_readers),
        footer_readers: Pool::new(footer_readers),
        superblock: Mutex::new(superblock),
        superblock_bytes: Mutex::new(Vec::new()),
        next_segment: AtomicU64::new(first_number),
        reserved_segment: AtomicU64::new(reserved),
        is_reserving: AtomicBool::new(false),
        deletes: AtomicU64::new(0),
        row_job_idle: AtomicBool::new(false),
        damaged_segments: AtomicU64::new(0),
        dropped_summary_rows: AtomicU64::new(0),
        memory_budget: config.memory_budget,
        seal_heap: AtomicU64::new(0),
        compaction_heap: AtomicU64::new(0),
        job_count: AtomicU64::new(0),
        row_task: OnceLock::new(),
        reserve_task: OnceLock::new(),
        seal_tasks,
        planner_task: OnceLock::new(),
        index_waiters: AtomicU64::new(0),
        cues: CueTable::new(),
        is_stopping: AtomicBool::new(false),
    }
}

