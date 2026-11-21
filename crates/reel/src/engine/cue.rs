//! A read view at one point in the write order, which copies no data and never blocks a writer
//!
//! A cue takes a slot in a fixed table, and everything that would drop a version or a cover counts itself and looks there first.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use reel_core::column_id::ColumnId;
use reel_core::direction::Direction;
use reel_core::reads::ValueWindow;
use reel_core::store_error::StoreError;
use reel_core::units::{ByteCount, RowCount, WritePlace};
use reel_core::value_list::ValueList;

use crate::engine::engine_state::EngineState;
use crate::engine::read_path::{ReadAt, RecordWant};
use crate::engine::scan_state::{from_key, in_range, under_prefix};
use crate::engine::store_scan::StoreScan;
use crate::io::{InPlace, Io, in_place};

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

/// The read points of every live cue, one a volume, since each volume publishes on its own mark
#[derive(Debug)]
pub struct CueTable {
    taken: [AtomicBool; MOST_CUES],
    points: Vec<AtomicU64>,
    volumes: usize,
    reclaims: AtomicU64,
    cues_gone: AtomicU64,
    frees: AtomicU64,
    refused_reads: AtomicU64,
}

impl CueTable {
    /// Start with no cues, on a store of this many volumes
    pub fn new(volumes: usize) -> Self {
        Self {
            taken: [const { AtomicBool::new(false) }; MOST_CUES],
            points: (0..MOST_CUES * volumes).map(|_point| AtomicU64::new(NO_CUE)).collect(),
            volumes,
            reclaims: AtomicU64::new(0),
            cues_gone: AtomicU64::new(0),
            frees: AtomicU64::new(0),
            refused_reads: AtomicU64::new(0),
        }
    }

    /// Take a free slot
    pub fn take_slot(&self) -> Option<usize> {
        self.taken.iter().position(|slot| slot.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst).is_ok())
    }

    /// Set a slot's read point on a volume
    pub fn set_point(&self, slot: usize, volume: usize, point: WritePlace) {
        self.points[slot * self.volumes + volume].store(point.0 + 1, Ordering::SeqCst);
    }

    /// Read a slot's point on a volume
    pub fn point(&self, slot: usize, volume: usize) -> WritePlace {
        WritePlace(self.points[slot * self.volumes + volume].load(Ordering::SeqCst).saturating_sub(1))
    }

    /// Take a slot for a read point on one volume, or return nothing when the table is full
    pub fn take(&self, volume: usize, point: WritePlace) -> Option<usize> {
        let slot = self.take_slot()?;
        self.set_point(slot, volume, point);
        Some(slot)
    }

    /// Give a slot back
    pub fn release(&self, slot: usize) {
        for volume in 0..self.volumes {
            self.points[slot * self.volumes + volume].store(NO_CUE, Ordering::SeqCst);
        }
        self.taken[slot].store(false, Ordering::SeqCst);
    }

    /// Run a test over the live points on a volume
    pub fn any_point(&self, volume: usize, test: impl Fn(u64) -> bool) -> bool {
        self.points.iter().skip(volume).step_by(self.volumes).any(|slot| {
            let point = slot.load(Ordering::SeqCst);
            point != NO_CUE && test(point - 1)
        })
    }

    /// Check a live cue reads below a place on a volume, so it can't see what stands at that place
    pub fn has_cue_below(&self, volume: usize, place: WritePlace) -> bool {
        self.any_point(volume, |point| point < place.0)
    }

    /// Check a live cue reads at or above one place and below another on a volume, so it sees a row the second one replaces
    pub fn has_cue_between(&self, volume: usize, lowest: WritePlace, highest: WritePlace) -> bool {
        self.any_point(volume, |point| (lowest.0..highest.0).contains(&point))
    }

    /// Check a live cue reads below a place, for a reclaim about to stand aside. The count moves before the last look, so
    /// a pin this finds sees the count move as it leaves, and asks for the reclaim again
    pub fn holds_back(&self, volume: usize, place: WritePlace) -> bool {
        if !self.has_cue_below(volume, place) {
            return false;
        }
        self.note_reclaim();
        self.has_cue_below(volume, place)
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
        self.taken.iter().filter(|slot| slot.load(Ordering::SeqCst)).count()
    }

    /// Check no cue is live
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// A read view at one point in the write order
///
/// A cue keeps every row its point reads, and while it lives a range delete written after it that hides rows in its own
/// segment stays in the pool, and that segment waits to merge until the cue goes
pub struct Cue<Backend: Io> {
    state: Arc<EngineState<Backend>>,
    slot: usize,
}

impl<Backend: Io> Cue<Backend> {
    /// Pin every volume's newest read point at one moment. The count comes first: a write that came after another that had
    /// returned took a higher place, so a cue holding the later one holds the earlier one, whatever order the marks are read
    /// in. A job counts itself before it looks at the table, and this reads the counts before taking a slot, so either the
    /// job sees the pin or this sees the job and takes a newer point
    pub fn new(state: Arc<EngineState<Backend>>) -> Result<Self, StoreError> {
        loop {
            let counts = state.cues.counts();
            let cut = state.places.last_given();
            if let Some(cue) = Self::pin_at(&state, counts, cut)? {
                return Ok(cue);
            }
        }
    }

    /// Pin a cue at a count read earlier, or return nothing when a job crossed it. No volume's point passes the count, so a write
    /// that took a place after it stays out on every volume
    pub fn pin_at(state: &Arc<EngineState<Backend>>, counts: JobCounts, cut: WritePlace) -> Result<Option<Self>, StoreError> {
        let Some(slot) = state.cues.take_slot() else {
            return Err(StoreError::CuesFull { limit: MOST_CUES as u64 });
        };
        for (index, volume) in state.volumes.iter().enumerate() {
            state.cues.set_point(slot, index, volume.order.mark().min(cut));
        }
        Ok(state.pin_holds(slot, counts).then(|| Self { state: Arc::clone(state), slot }))
    }

    /// Read the cue's point on a column's volume
    pub fn point_of(&self, column: ColumnId) -> Result<WritePlace, StoreError> {
        let info = self.state.column(column)?;
        Ok(self.state.cues.point(self.slot, info.volume.0 as usize))
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
        self.state.counted(self.state.contains_at(&self.state.key_read(column, key, self.point_of(column)?, true)?))
    }

    /// Answer every key at this cue, with a bad record as that key's answer
    pub fn get_many(&self, column: ColumnId, keys: &[&[u8]], values: &mut ValueList) -> Result<(), StoreError> {
        self.check_open()?;
        let info = self.state.column(column)?;
        let handovers = self.state.volumes[info.volume.0 as usize].handover_count();
        values.clear();
        let mut buffers = values.take_buffers();
        let read = in_place(self.state.read_keys(&InPlace, column, keys, ReadAt { point: self.point_of(column)?, handovers, trust: None }, values, &mut buffers));
        values.put_buffers(buffers);
        self.state.corruption_errors.fetch_add(values.corrupt_count(), Ordering::Relaxed);
        read
    }

    /// Walk rows and keys from a key at this cue
    pub fn iter_from(&self, column: ColumnId, start: &[u8], direction: Direction) -> Result<StoreScan<Backend>, StoreError> {
        self.check_open()?;
        StoreScan::at_cue(Arc::clone(&self.state), &from_key(column, start, direction), self.point_of(column)?)
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
        StoreScan::at_cue(Arc::clone(&self.state), &in_range(column, start, end, direction), self.point_of(column)?)
    }

    /// Walk rows and keys under a prefix at this cue
    pub fn iter_prefix(&self, column: ColumnId, prefix: &[u8]) -> Result<StoreScan<Backend>, StoreError> {
        self.check_open()?;
        StoreScan::at_cue(Arc::clone(&self.state), &under_prefix(column, prefix), self.point_of(column)?)
    }

    /// Count the keys under a prefix at this cue, reading no values
    pub fn count_prefix(&self, column: ColumnId, prefix: &[u8]) -> Result<RowCount, StoreError> {
        self.check_open()?;
        self.state.counted(self.state.count_at(column, prefix, self.point_of(column)?))
    }

    /// Add up the bytes the values under a prefix take on disk at this cue, reading no values
    pub fn bytes_prefix(&self, column: ColumnId, prefix: &[u8]) -> Result<ByteCount, StoreError> {
        self.check_open()?;
        self.state.counted(self.state.bytes_at(column, prefix, self.point_of(column)?))
    }

    /// Run a get at this cue without blocking the caller's runtime thread
    pub async fn read_async(&self, column: ColumnId, key: &[u8], want: RecordWant, buf: &mut Vec<u8>) -> Result<bool, StoreError> {
        self.check_open()?;
        self.state.read_async(column, key, want, Some(self.point_of(column)?), buf).await
    }

    /// Run a batched get at this cue without blocking the caller's runtime thread
    pub async fn many_async(&self, column: ColumnId, keys: &[&[u8]], values: &mut ValueList) -> Result<(), StoreError> {
        self.check_open()?;
        let info = self.state.column(column)?;
        let handovers = self.state.volumes[info.volume.0 as usize].handover_count();
        self.state.many_async(column, keys, self.point_of(column)?, handovers, values).await
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
        let read = self.state.key_read(column, key, self.point_of(column)?, true)?;
        self.state.counted(in_place(self.state.get_at(&InPlace, &read, handovers, want, buf)))
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
