//! A scan a caller holds: its buffers from the store's pool, and a cue's slot when it reads at one

use std::sync::Arc;

use reel_api::scan::ScanStep;
use reel_api::store_error::StoreError;
use reel_api::units::WritePlace;

use crate::engine::cue::MOST_CUES;
use crate::engine::engine_state::EngineState;
use crate::engine::scan_state::{ScanRange, ScanState};
use crate::io::Io;

/// A walk over keys in order. It holds no segment open between steps
pub struct StoreScan<Backend: Io> {
    state: Arc<EngineState<Backend>>,
    scan: Option<ScanState>,
    cue_slot: Option<usize>,
}

impl<Backend: Io> StoreScan<Backend> {
    /// Start a walk that answers each key at the newest point
    pub fn new(state: Arc<EngineState<Backend>>, range: &ScanRange) -> Result<Self, StoreError> {
        let scan = Some(state.take_scan(range, None)?);
        Ok(Self { state, scan, cue_slot: None })
    }

    /// Start a walk at a live cue's point. It takes a slot of its own, so it can outlive the cue
    pub fn at_cue(state: Arc<EngineState<Backend>>, range: &ScanRange, point: WritePlace) -> Result<Self, StoreError> {
        let Some(slot) = state.cues.take(point) else {
            return Err(StoreError::CuesFull { limit: MOST_CUES as u64 });
        };
        match state.take_scan(range, Some(point)) {
            Ok(scan) => Ok(Self { state, scan: Some(scan), cue_slot: Some(slot) }),
            Err(error) => {
                state.cues.release(slot);
                Err(error)
            },
        }
    }

    /// Fill the buffers with the next row
    pub fn next_row(&mut self, key: &mut Vec<u8>, value: &mut Vec<u8>) -> Result<ScanStep, StoreError> {
        let Some(scan) = &mut self.scan else {
            return Ok(ScanStep::End);
        };
        self.state.step_scan(scan, key, Some(value))
    }

    /// Fill the buffer with the next key
    pub fn next_key(&mut self, key: &mut Vec<u8>) -> Result<ScanStep, StoreError> {
        let Some(scan) = &mut self.scan else {
            return Ok(ScanStep::End);
        };
        self.state.step_scan(scan, key, None)
    }
}

impl<Backend: Io> Drop for StoreScan<Backend> {
    fn drop(&mut self) {
        if let Some(scan) = self.scan.take() {
            self.state.give_back_scan(scan);
        }
        if let Some(slot) = self.cue_slot {
            self.state.cues.release(slot);
            // what this walk kept can go now, and nothing else would look again
            self.state.ask_compaction();
        }
    }
}
