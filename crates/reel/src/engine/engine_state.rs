//! Everything a store shares between its threads

use std::io::Error;
use std::mem;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock, RwLock};

use reel_api::column_id::ColumnId;
use reel_api::store_error::StoreError;
use reel_api::units::{ByteCount, WritePlace};

use crate::engine::column_info::ColumnInfo;
use crate::engine::engine_sizing::EngineSizing;
use crate::engine::open_segment::RowQueue;
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
    pub slot_files: Vec<SlotFile<Backend::File>>,
    pub free_slots: Mutex<Vec<OpenSlot>>,
    pub encoders: Pool<RecordEncoder>,
    pub large_encoders: Pool<RecordEncoder>,
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
    pub row_task: OnceLock<Backend::Task>,
    pub reserve_task: OnceLock<Backend::Task>,
    pub seal_tasks: Vec<OnceLock<Backend::Task>>,
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
        Self {
            data: RwLock::new(None),
            leaving_rows: Mutex::new(None),
            pins: AtomicU32::new(0),
        }
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

    /// Add up the heap the shared parts hold
    pub fn heap_bytes(&self) -> ByteCount {
        let mut summaries = 0;
        for volume in &self.volumes {
            summaries += hold(&volume.summary).heap_bytes().0;
        }
        ByteCount(self.index.heap_bytes().0 + self.spot.heap_bytes().0 + summaries)
    }
}

