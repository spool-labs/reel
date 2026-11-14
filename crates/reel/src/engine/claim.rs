//! A writer's claim on bytes and places, which ends cleanly however the writer does

use std::sync::atomic::Ordering;

use reel_api::units::{ByteCount, FileOffset, RowCount, WritePlace};

use crate::engine::engine_state::EngineState;
use crate::engine::open_segment::SegmentPhase;
use crate::engine::volume::Volume;
use crate::guard::hold;
use crate::index::open_slots::OpenSlot;
use crate::io::Io;
use crate::segment::segment_tail::SegmentNumber;
use crate::write_order::Ticket;

/// A byte range and a run of places, claimed together
pub struct Claim<'state, Backend: Io> {
    pub state: &'state EngineState<Backend>,
    pub volume: &'state Volume<Backend>,
    pub slot: OpenSlot,
    pub segment: SegmentNumber,
    pub data: Backend::File,
    pub offset: FileOffset,
    pub ticket: Ticket,
    /// The rows, covers and bytes the claim added to its segment. A claim that ends without landing gives the rows and covers
    /// back, and its bytes stay behind as a gap the gauge counts
    pub rows: RowCount,
    pub covers: usize,
    pub bytes: ByteCount,
    pub is_landed: bool,
    pub is_alone: bool,
}

impl<Backend: Io> Claim<'_, Backend> {
    /// Read the claim's first place
    pub fn first_place(&self) -> WritePlace {
        self.ticket.first
    }

    /// End the claim with its rows in the index, and wait until a later read sees them
    pub fn land(self) {
        let volume = self.volume;
        let ticket = self.ticket;
        self.finish();
        volume.order.wait_visible(&ticket);
    }

    /// End the claim with its rows in the index, leaving the wait for the mark to the caller
    pub fn finish(mut self) {
        self.volume.order.finish(&self.ticket);
        self.is_landed = true;
        self.release();
    }

    /// Let the segment seal once its last claim is done
    pub fn release(&self) {
        let mut claims = hold(&self.volume.claims);
        for segment in &mut claims.open {
            if segment.slot == self.slot {
                segment.in_flight -= 1;
                if !self.is_landed {
                    segment.row_count = RowCount(segment.row_count.0 - self.rows.0);
                    segment.cover_rows -= self.covers;
                    self.volume.rows_in_index.fetch_sub(self.rows.0, Ordering::Relaxed);
                    self.volume.unlanded_bytes.fetch_add(self.bytes.0, Ordering::Relaxed);
                }
                self.volume.progress.wake();
                if self.is_alone && segment.phase == SegmentPhase::Alone {
                    segment.phase = SegmentPhase::Taking;
                }
                break;
            }
        }
        if self.is_alone {
            self.state.mark_full_segments(&mut claims, self.volume);
        }
        self.state.slot_files[self.slot.0 as usize].pins.fetch_sub(1, Ordering::AcqRel);
    }
}

impl<Backend: Io> Drop for Claim<'_, Backend> {
    fn drop(&mut self) {
        if !self.is_landed {
            // a failed writer must not hold the mark, or every later write would wait for it
            self.volume.order.finish(&self.ticket);
            self.release();
        }
    }
}
