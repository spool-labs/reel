//! A store: open, the calls consumers make, and close

use std::io::Error;
use std::mem;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use reel_api::column_id::ColumnId;
use reel_api::reads::ValueWindow;
use reel_api::store_error::StoreError;
use reel_api::units::{ByteCount, VolumeId, WritePlace};

use crate::config::{ColumnConfig, StoreConfig};
use crate::engine::column_info::ColumnInfo;
use crate::engine::engine_sizing::{
    EngineSizing, FILTER_CACHE_QUARTERS, FILTER_SLOT_BYTES, GROUP_BYTES, LEAST_LZ4_BUDGET, READ_BUFFERS, WRITE_BUFFERS,
};
use crate::engine::engine_state::{EngineState, PLACE_RUN, SEGMENT_NUMBER_BATCH, SlotFile};
use crate::engine::open_segment::RowQueue;
use crate::engine::read_path::RecordWant;
use crate::engine::recovery::{
    RootScan, SummaryFound, list_roots, read_summaries, recover_segments, save_cleared_spans, scan_roots,
    settle_summaries,
};
use crate::engine::reserve_job::ReserveJob;
use crate::engine::row_job::RowJob;
use crate::engine::seal_job::SealJob;
use crate::engine::volume::{ClaimState, SyncState, Volume};
use crate::guard::hold;
use crate::index::open_slots::OpenSlot;
use crate::index::spot_index::SpotIndex;
use crate::index::write_index::WriteIndex;
use crate::io::lock_outcome::LockOutcome;
use crate::io::{Io, IoTask};
use crate::pool::Pool;
use crate::record::codec::{CHUNK_BYTES, Codec};
use crate::record::record_check::RecordKey;
use crate::record::record_encoder::RecordEncoder;
use crate::record::record_reader::RecordReader;
use crate::rows::row_group::RowGroupBuffer;
use crate::segment::block_cache::BlockCache;
use crate::segment::column_directory::ColumnDirectory;
use crate::segment::damage_list::DamageList;
use crate::segment::summary::{MOST_ROW_COLUMN_BYTES, MOST_ROW_FIXED_BYTES};
use crate::segment::footer_reader::FooterReader;
use crate::segment::row_block::ROW_BLOCK_BYTES;
use crate::segment::segment_table::{SegmentTable, TABLE_SHARDS, TableSizing};
use crate::segment::segment_tail::{MAX_COLUMNS, SegmentNumber};
use crate::superblock::{StoreId, StoredColumn, StoredVolume, Superblock, SuperblockPaths, stored_column};
use crate::write_order::WriteOrder;

/// The lock file in each root
pub const LOCK_NAME: &str = "lock";
/// Segments a table holds per byte of the budget, as a divisor
pub const TABLE_BUDGET_DIVISOR: u64 = 16;
/// Bytes one segment is planned at in the table
pub const PLANNED_SEGMENT_BYTES: u64 = 1_024;
/// Block lines a table's share is planned for, as bytes per line
pub const PLANNED_LINE_BYTES: u64 = 32;

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
        let sizing = EngineSizing::for_budget(config.memory_budget, root_count);

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

    /// Check whether the key has a value, without reading the value
    pub fn contains(&self, column: ColumnId, key: &[u8]) -> Result<bool, StoreError> {
        self.check_open()?;
        self.state.contains_at(column, key, self.state.read_point())
    }

    /// Clear a damaged span once the caller has repaired it from peers, up to an end key or the column's end
    pub fn clear_damage(&self, column: ColumnId, start: &[u8], end: Option<&[u8]>) -> Result<(), StoreError> {
        self.check_open()?;
        let info = self.state.column(column)?;
        let volume = &self.state.volumes[info.volume.0 as usize];
        volume.damage.clear(column, start, end);
        save_cleared_spans(&self.state, volume);
        self.state.save_summary(volume).map_err(open_error("saving a summary"))
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
        if let Some(task) = self.row_task.get() {
            task.stop()?;
        }
        if let Some(task) = self.reserve_task.get() {
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
    settle_summaries(state, scan).map_err(open_error("settling a summary"))?;

    scan.sealed.reverse();
    while let Some(found) = scan.sealed.last() {
        let volume = &state.volumes[found.volume];
        state.install_sealed(volume, found).map_err(open_error("installing a segment"))?;
        scan.sealed.pop();
    }
    let highest_segment = scan.highest_segment;
    recover_segments(state, &mut scan.open, highest_segment).map_err(open_error("recovering a segment"))?;
    let damaged: usize = state.volumes.iter().map(|volume| hold(&volume.damaged).len()).sum();
    state.damaged_segments.store(damaged as u64, Ordering::Release);

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
        let task = state.io.spawn_job("reel-seal", seal_job).map_err(open_error("starting a job"))?;
        let _set = state.seal_tasks[volume].set(task);
    }
    let reserve_job = ReserveJob {
        state: Arc::clone(state),
    };
    let task = state.io.spawn_job("reel-reserve", reserve_job).map_err(open_error("starting a job"))?;
    let _set = state.reserve_task.set(task);
    Ok(())
}

/// Refuse a config that can't work
pub fn check_config(config: &StoreConfig) -> Result<(), StoreError> {
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
    for volume in &config.volumes {
        for root in &volume.roots {
            let found = Superblock::read(io, root).map_err(open_error("reading a superblock"))?;
            let Some(found) = found else {
                continue;
            };
            highest_limit = highest_limit.max(found.place_limit);
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
    let table_share = config.memory_budget.0 / TABLE_BUDGET_DIVISOR;
    let segments_per_shard = ((table_share / PLANNED_SEGMENT_BYTES) as usize / TABLE_SHARDS).max(8);
    let table_sizing = TableSizing {
        segments_per_shard,
        columns_per_shard: segments_per_shard * config.columns.len().max(1),
        blocks_per_shard: ((table_share / PLANNED_LINE_BYTES) as usize / TABLE_SHARDS).max(64),
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
        let planned_summary = sizing.summary_share.0 as usize * row_room(columns_here) / all_row_room;
        // open never drops a row, so a segment's damage keeps its rank through every open
        let summary_room = found_summary.max(planned_summary).max(scan.least_summary_room(index, columns_here) * 2);
        summary.rows.make_room(ByteCount(summary_room as u64));
        volumes.push(Volume {
            id: VolumeId(index as u8),
            name: volume.name.clone(),
            roots: volume.roots.clone(),
            superblock_paths: paths,
            policy: volume.sync,
            locks: Mutex::new(held),
            claims: Mutex::new(ClaimState {
                open: Vec::with_capacity(open_slots),
                is_creating: false,
                bytes_since_sync: ByteCount(0),
                next_root: 0,
            }),
            table: SegmentTable::new(&table_room(&table_sizing, &scan.least_sizing(index))),
            view: AtomicU64::new(0),
            handovers: AtomicU64::new(0),
            highest_sealed: AtomicU64::new(0),
            sync: Mutex::new(SyncState::default()),
            is_failed: AtomicBool::new(false),
            segment_bytes: sizing.segment_bytes,
            segment_rows: sizing.segment_rows,
            reserve: ByteCount(sizing.segment_bytes.0 * 2),
            summary: Mutex::new(summary.rows),
            summary_paths: summary.paths,
            damage: DamageList::new(),
            damaged: Mutex::new(Vec::new()),
        });
    }

    let mut queues = Vec::with_capacity(open_slots);
    let mut spare_groups = Vec::with_capacity(open_slots);
    let mut slot_files = Vec::with_capacity(open_slots);
    let mut free_slots = Vec::with_capacity(open_slots);
    for slot in 0..open_slots {
        queues.push(Mutex::new(RowQueue::new(GROUP_BYTES)));
        spare_groups.push(Mutex::new(Some(RowGroupBuffer::new(GROUP_BYTES))));
        slot_files.push(SlotFile::new());
        free_slots.push(OpenSlot((open_slots - 1 - slot) as u8));
    }

    let largest_value = sizing.segment_bytes;
    let mut encoders = Vec::with_capacity(WRITE_BUFFERS);
    for _encoder in 0..WRITE_BUFFERS {
        encoders.push(RecordEncoder::new(largest_value, ByteCount(CHUNK_BYTES as u64)));
    }
    let large_stored = if has_lz4 { sizing.lz4_stored_limit } else { ByteCount(CHUNK_BYTES as u64) };
    let large_encoders = vec![RecordEncoder::new(largest_value, large_stored)];

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
        slot_files,
        free_slots: Mutex::new(free_slots),
        encoders: Pool::new(encoders),
        large_encoders: Pool::new(large_encoders),
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
        row_task: OnceLock::new(),
        reserve_task: OnceLock::new(),
        seal_tasks,
    }
}

