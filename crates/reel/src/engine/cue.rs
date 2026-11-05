//! A read view at one point in the write order, which copies no data and never blocks a writer
//!
//! A cue takes a slot in a fixed table, and everything that would drop a version or a cover looks there first.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use reel_api::column_id::ColumnId;
use reel_api::reads::ValueWindow;
use reel_api::store_error::StoreError;
use reel_api::units::WritePlace;
use reel_api::value_list::ValueList;

use crate::engine::engine_state::EngineState;
use crate::engine::read_path::RecordWant;
use crate::io::Io;

/// The most cues a store holds at once
pub const MOST_CUES: usize = 64;
/// A slot with no cue in it, since a place plus one is never zero
pub const NO_CUE: u64 = 0;

/// The read point of every live cue
#[derive(Debug)]
pub struct CueTable {
    slots: [AtomicU64; MOST_CUES],
}

impl CueTable {
    /// Start with no cues
    pub fn new() -> Self {
        Self {
            slots: [const { AtomicU64::new(NO_CUE) }; MOST_CUES],
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
    /// Pin the newest read point. A cover drop counts itself before it looks at the table, and this reads the count after
    /// taking a slot, so either the drop sees the pin or this sees the drop and takes a newer point
    pub fn new(state: Arc<EngineState<Backend>>) -> Result<Self, StoreError> {
        loop {
            let drops = state.cover_drops();
            let point = state.read_point();
            let Some(slot) = state.cues.take(point) else {
                return Err(StoreError::CuesFull { limit: MOST_CUES as u64 });
            };
            if state.cover_drops() == drops {
                return Ok(Self { state, slot, point });
            }
            state.cues.release(slot);
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
        self.state.contains_at(&self.state.key_read(column, key, self.point, true)?)
    }

    /// Answer every key at this cue, with a bad record as that key's answer
    pub fn get_many(&self, column: ColumnId, keys: &[&[u8]], values: &mut ValueList) -> Result<(), StoreError> {
        let info = self.state.column(column)?;
        let handovers = self.state.volumes[info.volume.0 as usize].handover_count();
        values.clear();
        let mut scratch = values.take_scratch();
        let read = self.state.read_keys(column, keys, self.point, handovers, values, &mut scratch);
        values.put_scratch(scratch);
        read
    }

    /// Read a key at this cue
    pub fn read(&self, column: ColumnId, key: &[u8], want: RecordWant, buf: &mut Vec<u8>) -> Result<bool, StoreError> {
        let info = self.state.column(column)?;
        let handovers = self.state.volumes[info.volume.0 as usize].handover_count();
        let read = self.state.key_read(column, key, self.point, true)?;
        self.state.get_at(&read, handovers, want, buf)
    }
}

impl<Backend: Io> Drop for Cue<Backend> {
    fn drop(&mut self) {
        self.state.cues.release(self.slot);
    }
}
