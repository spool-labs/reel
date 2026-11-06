//! A read view at one point in the write order, which copies no data and never blocks a writer
//!
//! A cue takes a slot in a fixed table, and everything that would drop a version or a cover counts itself and looks there first.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use reel_api::column_id::ColumnId;
use reel_api::direction::Direction;
use reel_api::reads::ValueWindow;
use reel_api::store_error::StoreError;
use reel_api::units::{ByteCount, RowCount, WritePlace};
use reel_api::value_list::ValueList;

use crate::engine::engine_state::EngineState;
use crate::engine::read_path::RecordWant;
use crate::engine::scan_state::{from_key, in_range, under_prefix};
use crate::engine::store_scan::StoreScan;
use crate::io::Io;

/// The most cues a store holds at once. A get_many holds a slot while it reads, so callers inside one count toward this
pub const MOST_CUES: usize = 64;
/// A slot with no cue in it, since a place plus one is never zero
pub const NO_CUE: u64 = 0;

/// The job counts a read takes before its point, and compares once it has an answer
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct JobCounts {
    /// Rows that left the lists, which can hide a row from any read
    pub reclaims: u64,
    /// Records freed in place, which can only fail a read's record check
    pub frees: u64,
}

/// The read point of every live cue
#[derive(Debug)]
pub struct CueTable {
    slots: [AtomicU64; MOST_CUES],
    reclaims: AtomicU64,
    cues_gone: AtomicU64,
    frees: AtomicU64,
    refused_reads: AtomicU64,
}

impl CueTable {
    /// Start with no cues
    pub fn new() -> Self {
        Self {
            slots: [const { AtomicU64::new(NO_CUE) }; MOST_CUES],
            reclaims: AtomicU64::new(0),
            cues_gone: AtomicU64::new(0),
            frees: AtomicU64::new(0),
            refused_reads: AtomicU64::new(0),
        }
    }

    /// Take a slot for a read point, or return nothing when the table is full
    pub fn take(&self, point: WritePlace) -> Option<usize> {
        self.slots.iter().position(|slot| {
            slot.compare_exchange(NO_CUE, point.0 + 1, Ordering::SeqCst, Ordering::SeqCst).is_ok()
        })
    }

    /// Give a slot back
    pub fn release(&self, slot: usize) {
        self.slots[slot].store(NO_CUE, Ordering::SeqCst);
    }

    /// Check a live cue reads below a place, so it can't see what stands at that place
    pub fn has_cue_below(&self, place: WritePlace) -> bool {
        self.slots.iter().any(|slot| {
            let point = slot.load(Ordering::SeqCst);
            point != NO_CUE && point - 1 < place.0
        })
    }

    /// Check a live cue reads at or above one place and below another, so it sees a row the second one replaced
    pub fn has_cue_between(&self, lowest: WritePlace, highest: WritePlace) -> bool {
        self.slots.iter().any(|slot| {
            let point = slot.load(Ordering::SeqCst);
            point != NO_CUE && (lowest.0..highest.0).contains(&(point - 1))
        })
    }

    /// Check a live cue reads below a place, for a reclaim about to stand aside. The count moves before the last look, so
    /// a pin this finds sees the count move as it leaves, and asks for the reclaim again
    pub fn holds_back(&self, place: WritePlace) -> bool {
        if !self.has_cue_below(place) {
            return false;
        }
        self.note_reclaim();
        self.has_cue_below(place)
    }

    /// Count a reclaim, before its last look at this table and before anything leaves a directory. A reader that took its
    /// point after this has every replacement under it, and one that took it before sees this count move
    pub fn note_reclaim(&self) {
        self.reclaims.fetch_add(1, Ordering::SeqCst);
    }

    /// Count a free in place, before its last look at this table and before it touches a block. A reader that took its
    /// point after this has the replacement under it, and one that took it before and fails a record check sees this move
    pub fn note_free(&self) {
        self.frees.fetch_add(1, Ordering::SeqCst);
    }

    /// Read the job counts
    pub fn counts(&self) -> JobCounts {
        JobCounts {
            reclaims: self.reclaims.load(Ordering::SeqCst),
            frees: self.frees.load(Ordering::SeqCst),
        }
    }

    /// Count a cue going, since a row or grave it held can go now
    pub fn note_cue_gone(&self) {
        self.cues_gone.fetch_add(1, Ordering::AcqRel);
    }

    /// Read how many cues have gone
    pub fn cues_gone(&self) -> u64 {
        self.cues_gone.load(Ordering::Acquire)
    }

    /// Count a read that had to give up because jobs kept crossing it and the table had no slot for a pin
    pub fn note_refused_read(&self) {
        self.refused_reads.fetch_add(1, Ordering::Relaxed);
    }

    /// Read how many reads have given up
    pub fn refused_reads(&self) -> u64 {
        self.refused_reads.load(Ordering::Relaxed)
    }

    /// Count the live cues
    pub fn len(&self) -> usize {
        self.slots.iter().filter(|slot| slot.load(Ordering::SeqCst) != NO_CUE).count()
    }

    /// Check no cue is live
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Default for CueTable {
    fn default() -> Self {
        Self::new()
    }
}

/// A read view at one point in the write order
pub struct Cue<Backend: Io> {
    state: Arc<EngineState<Backend>>,
    slot: usize,
    point: WritePlace,
}

impl<Backend: Io> Cue<Backend> {
    /// Pin the newest read point. A job counts itself before it looks at the table, and this reads the counts after
    /// taking a slot, so either the job sees the pin or this sees the job and takes a newer point
    pub fn new(state: Arc<EngineState<Backend>>) -> Result<Self, StoreError> {
        loop {
            let counts = state.cues.counts();
            let point = state.read_point();
            let Some(slot) = state.cues.take(point) else {
                return Err(StoreError::CuesFull { limit: MOST_CUES as u64 });
            };
            if state.pin_holds(slot, counts) {
                return Ok(Self { state, slot, point });
            }
        }
    }

    /// Read the cue's point in the write order
    pub fn point(&self) -> WritePlace {
        self.point
    }

    /// Fill the buffer with the value the key had at this cue, and say whether it had one
    pub fn get(&self, column: ColumnId, key: &[u8], buf: &mut Vec<u8>) -> Result<bool, StoreError> {
        self.read(column, key, RecordWant::Whole, buf)
    }

    /// Fill the buffer with a checked window of the value the key had at this cue
    pub fn get_range(
        &self,
        column: ColumnId,
        key: &[u8],
        window: ValueWindow,
        buf: &mut Vec<u8>,
    ) -> Result<bool, StoreError> {
        self.read(column, key, RecordWant::Window(window), buf)
    }

    /// Check whether the key had a value at this cue
    pub fn contains(&self, column: ColumnId, key: &[u8]) -> Result<bool, StoreError> {
        self.check_open()?;
        self.state.counted(self.state.contains_at(&self.state.key_read(column, key, self.point, true)?))
    }

    /// Answer every key at this cue, with a bad record as that key's answer
    pub fn get_many(&self, column: ColumnId, keys: &[&[u8]], values: &mut ValueList) -> Result<(), StoreError> {
        self.check_open()?;
        let info = self.state.column(column)?;
        let handovers = self.state.volumes[info.volume.0 as usize].handover_count();
        values.clear();
        let mut scratch = values.take_scratch();
        let read = self.state.read_keys(column, keys, self.point, handovers, values, &mut scratch);
        values.put_scratch(scratch);
        self.state.corruption_errors.fetch_add(values.corrupt_count(), Ordering::Relaxed);
        read
    }

    /// Walk rows and keys from a key at this cue
    pub fn iter_from(&self, column: ColumnId, start: &[u8], direction: Direction) -> Result<StoreScan<Backend>, StoreError> {
        self.check_open()?;
        StoreScan::at_cue(Arc::clone(&self.state), &from_key(column, start, direction), self.point)
    }

    /// Walk rows and keys in [start, end) at this cue
    pub fn iter_range(
        &self,
        column: ColumnId,
        start: &[u8],
        end: &[u8],
        direction: Direction,
    ) -> Result<StoreScan<Backend>, StoreError> {
        self.check_open()?;
        StoreScan::at_cue(Arc::clone(&self.state), &in_range(column, start, end, direction), self.point)
    }

    /// Walk rows and keys under a prefix at this cue
    pub fn iter_prefix(&self, column: ColumnId, prefix: &[u8]) -> Result<StoreScan<Backend>, StoreError> {
        self.check_open()?;
        StoreScan::at_cue(Arc::clone(&self.state), &under_prefix(column, prefix), self.point)
    }

    /// Count the keys under a prefix at this cue, reading no values
    pub fn count_prefix(&self, column: ColumnId, prefix: &[u8]) -> Result<RowCount, StoreError> {
        self.check_open()?;
        self.state.counted(self.state.count_at(column, prefix, self.point))
    }

    /// Add up the bytes the values under a prefix take on disk at this cue, reading no values
    pub fn bytes_prefix(&self, column: ColumnId, prefix: &[u8]) -> Result<ByteCount, StoreError> {
        self.check_open()?;
        self.state.counted(self.state.bytes_at(column, prefix, self.point))
    }

    /// Run a get at this cue without blocking the caller's runtime thread
    pub async fn read_async(&self, column: ColumnId, key: &[u8], want: RecordWant, buf: &mut Vec<u8>) -> Result<bool, StoreError> {
        self.check_open()?;
        self.state.read_async(column, key, want, Some(self.point), buf).await
    }

    /// Run a batched get at this cue without blocking the caller's runtime thread
    pub async fn many_async(&self, column: ColumnId, keys: &[&[u8]], values: &mut ValueList) -> Result<(), StoreError> {
        self.check_open()?;
        let info = self.state.column(column)?;
        let handovers = self.state.volumes[info.volume.0 as usize].handover_count();
        self.state.many_async(column, keys, self.point, handovers, values).await
    }

    /// Refuse a read once the store is going, whose files are closing under it
    pub fn check_open(&self) -> Result<(), StoreError> {
        if self.state.is_stopping.load(Ordering::Acquire) {
            return Err(StoreError::Closed);
        }
        Ok(())
    }

    /// Read a key at this cue
    pub fn read(&self, column: ColumnId, key: &[u8], want: RecordWant, buf: &mut Vec<u8>) -> Result<bool, StoreError> {
        self.check_open()?;
        let info = self.state.column(column)?;
        let handovers = self.state.volumes[info.volume.0 as usize].handover_count();
        let read = self.state.key_read(column, key, self.point, true)?;
        self.state.counted(self.state.get_at(&read, handovers, want, buf))
    }
}

impl<Backend: Io> Drop for Cue<Backend> {
    fn drop(&mut self) {
        self.state.cues.release(self.slot);
        self.state.cues.note_cue_gone();
        // what this cue kept can go now, and nothing else would look again
        self.state.ask_compaction();
    }
}
