//! Everything a store shares between its threads

use std::io::Error;
use std::mem;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock, RwLock};

use reel_api::column_id::ColumnId;
use reel_api::memory_report::MemoryReport;
use reel_api::store_error::StoreError;
use reel_api::units::{ByteCount, WritePlace};

use crate::engine::column_info::ColumnInfo;
use crate::engine::engine_sizing::EngineSizing;
use crate::engine::open_segment::{RecordWindow, RowQueue};
use crate::engine::volume::Volume;
use crate::guard::hold;
use crate::index::open_slots::OpenSlot;
use crate::index::spot_index::SpotIndex;
use crate::index::write_index::WriteIndex;
use crate::io::Io;
use crate::pool::Pool;
use crate::record::record_check::RecordKey;
use crate::record::record_encoder::RecordEncoder;
use crate::record::record_reader::RecordReader;
use crate::rows::row_group::RowGroupBuffer;
use crate::segment::block_cache::BlockCache;
use crate::segment::column_directory::ColumnDirectory;
use crate::segment::footer_reader::FooterReader;
use crate::segment::segment_tail::SegmentNumber;
use crate::segment::summary::write_summary;
use crate::superblock::Superblock;
use crate::write_order::WriteOrder;

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
}

/// The state every store handle and job shares
pub struct EngineState<Backend: Io> {
    pub io: Backend,
    pub record_key: RecordKey,
    pub sizing: EngineSizing,
    pub columns: Vec<Option<ColumnInfo>>,
    pub directories: Vec<Option<ColumnDirectory>>,
    pub volumes: Vec<Volume<Backend>>,
    pub order: WriteOrder,
    pub index: WriteIndex,
    pub spot: SpotIndex,
    pub filter_cache: BlockCache,
    pub row_cache: BlockCache,
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
    pub longest_turn: AtomicU64,
    pub index_waits: AtomicU64,
    pub longest_index_wait: AtomicU64,
    pub record_readers: Pool<RecordReader>,
    pub footer_readers: Pool<FooterReader>,
    pub superblock: Mutex<Superblock>,
    pub superblock_bytes: Mutex<Vec<u8>>,
    pub next_segment: AtomicU64,
    pub reserved_segment: AtomicU64,
    pub is_reserving: AtomicBool,
    pub deletes: AtomicU64,
    pub row_job_idle: AtomicBool,
    pub damaged_segments: AtomicU64,
    pub dropped_summary_rows: AtomicU64,
    pub memory_budget: ByteCount,
    pub seal_heap: AtomicU64,
    pub job_count: AtomicU64,
    pub row_task: OnceLock<Backend::Task>,
    pub reserve_task: OnceLock<Backend::Task>,
    pub seal_tasks: Vec<OnceLock<Backend::Task>>,
    pub planner_task: OnceLock<Backend::Task>,
    pub index_waiters: AtomicU64,
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
        };
        // some platforms set a lock up at its first use, and that's a job for open
        drop(hold(&slot.leaving_rows));
        slot
    }

    /// Read the file the slot holds now
    pub fn file(&self) -> Option<File> {
        let guard = match self.data.read() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        guard.as_ref().map(|held| held.file.clone())
    }

    /// Hold a segment's file for a read, or return nothing when the slot has moved on from it
    pub fn pin(&self, segment: SegmentNumber) -> Option<SlotPin<'_, File>> {
        let guard = match self.data.read() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
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
            Err(poisoned) => poisoned.into_inner(),
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

    /// Read the write mark a new read starts from
    pub fn read_point(&self) -> WritePlace {
        self.order.mark()
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
        volume.summary_buffer.with(|buffer| {
            buffer.clear();
            buffer.extend_from_slice(hold(&volume.summary).bytes());
            write_summary(&self.io, &volume.summary_paths, self.read_point(), buffer)
        })
    }

    /// Check the saved limit or segment counter is more than half used
    pub fn needs_reservation(&self) -> bool {
        let next_segment = self.next_segment.load(Ordering::Acquire);
        let reserved = self.reserved_segment.load(Ordering::Acquire);
        self.order.places_left() < PLACE_RUN / 2 || reserved.saturating_sub(next_segment) < SEGMENT_NUMBER_BATCH / 2
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
            self.order.set_limit(limit);
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
                self.io.pause();
            }
        }
        Ok(SegmentNumber(number))
    }

    /// Take a free open slot
    pub fn take_slot(&self) -> Option<OpenSlot> {
        hold(&self.free_slots).pop()
    }

    /// Give a slot back once nothing uses it
    pub fn free_slot(&self, slot: OpenSlot) {
        hold(&self.free_slots).push(slot);
    }

    /// Count a volume's segments waiting for the dead-byte walk
    pub fn pending_walks(&self, volume: usize) -> usize {
        hold(&self.volumes[volume].summary).rows().filter(|row| !row.facts.is_walked()).count()
    }

    /// Count the times a volume's walk has had to try a block again
    pub fn walk_retries(&self, volume: usize) -> u64 {
        self.volumes[volume].walk_retries.load(Ordering::Relaxed)
    }

    /// Report the heap each structure holds
    pub fn memory_report(&self) -> MemoryReport {
        let mut report = MemoryReport {
            budget: self.memory_budget,
            write_index: self.index.heap_bytes(),
            spot_cache: self.spot.heap_bytes(),
            block_cache: ByteCount(self.filter_cache.heap_bytes().0 + self.row_cache.heap_bytes().0),
            footer_space: ByteCount(self.seal_heap.load(Ordering::Acquire)),
            thread_stacks: ByteCount(self.job_count.load(Ordering::Acquire) * self.io.job_stack_bytes().0),
            ..MemoryReport::default()
        };
        for directory in self.directories.iter().flatten() {
            report.block_indexes = ByteCount(report.block_indexes.0 + directory.heap_bytes().0);
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
        report.read_buffers = ByteCount(readers.0 + footers.0);
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
