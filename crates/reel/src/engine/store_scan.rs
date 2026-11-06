//! A scan a caller holds: its buffers from the store's pool, and a cue's slot when it reads at one

use std::mem;
use std::sync::Arc;

use reel_api::scan::ScanStep;
use reel_api::store_error::StoreError;
use reel_api::units::WritePlace;
use reel_api::value_list::ValueList;

use crate::engine::cue::MOST_CUES;
use crate::engine::engine_state::EngineState;
use crate::engine::read_pool::ReadWork;
use crate::engine::scan_path::ScanWant;
use crate::engine::scan_state::{ScanRange, ScanState};
use crate::io::Io;
use crate::io::ring::OnRing;

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
        let info = state.column(range.column)?;
        let Some(slot) = state.cues.take(info.volume.0 as usize, point) else {
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
        self.state.step_scan(scan, key, ScanWant::Value(value))
    }

    /// Stream the next block of keys through the read workers, and say whether the walk has more. A walk dropped part way
    /// gives its buffers back when the worker ends, and a future dropped part way ends the walk
    pub async fn next_block(&mut self, keys: &mut ValueList) -> Result<bool, StoreError> {
        keys.clear();
        if self.state.ring.is_some() {
            return self.next_block_on_ring(keys).await;
        }
        let lease = self.state.read_pool.claim().await;
        let Some(scan) = self.scan.take() else {
            return Ok(false);
        };
        lease.set_work(ReadWork::Block { scan, keys: mem::take(keys) });
        self.state.wake_reader(lease.submit());
        let (work, answer) = lease.finished().await;
        if let ReadWork::Block { scan, keys: list } = work {
            self.scan = Some(scan);
            *keys = list;
        }
        answer
    }

    /// Stream the next block of keys on the ring, on the caller's task. A future dropped part way ends the walk, since a step
    /// that stopped half done leaves nothing to go on from
    pub async fn next_block_on_ring(&mut self, keys: &mut ValueList) -> Result<bool, StoreError> {
        let state = Arc::clone(&self.state);
        let Some(ring) = &state.ring else {
            return Ok(false);
        };
        let Ok(_seat) = ring.seat().await else {
            return Err(StoreError::Closed);
        };
        let Some(scan) = self.scan.take() else {
            return Ok(false);
        };
        let mut held = HeldScan { state: &state, scan: Some(scan) };
        let answer = match &mut held.scan {
            Some(scan) => state.scan_block_on(&OnRing(ring), scan, keys).await,
            None => Ok(false),
        };
        self.scan = held.scan.take();
        answer
    }

    /// Fill the buffer with the next key
    pub fn next_key(&mut self, key: &mut Vec<u8>) -> Result<ScanStep, StoreError> {
        let Some(scan) = &mut self.scan else {
            return Ok(ScanStep::End);
        };
        self.state.step_scan(scan, key, ScanWant::Key)
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

/// A scan on its way through a ring read, which goes back to the pool when the read is dropped part way
pub struct HeldScan<'state, Backend: Io> {
    state: &'state EngineState<Backend>,
    scan: Option<ScanState>,
}

impl<Backend: Io> Drop for HeldScan<'_, Backend> {
    fn drop(&mut self) {
        if let Some(scan) = self.scan.take() {
            self.state.give_back_scan(scan);
        }
    }
}
