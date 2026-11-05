//! A writer's claim on bytes and places, which ends cleanly however the writer does

use std::sync::atomic::Ordering;

use reel_api::units::{FileOffset, WritePlace};

use crate::engine::engine_state::EngineState;
use crate::engine::volume::Volume;
use crate::guard::hold;
use crate::index::open_slots::OpenSlot;
use crate::io::Io;
use crate::segment::segment_tail::SegmentNumber;
use crate::write_order::{Ticket, TicketState};

/// A byte range and a run of places, claimed together
pub struct Claim<'state, Backend: Io> {
    pub state: &'state EngineState<Backend>,
    pub volume: &'state Volume<Backend>,
    pub slot: OpenSlot,
    pub segment: SegmentNumber,
    pub data: Backend::File,
    pub offset: FileOffset,
    pub ticket: Ticket,
    pub is_landed: bool,
}

impl<Backend: Io> Claim<'_, Backend> {
    /// Read the claim's first place
    pub fn first_place(&self) -> WritePlace {
        self.ticket.first
    }

    /// End the claim with its rows in the index, and wait until a later read sees them
    pub fn land(self) {
        let state = self.state;
        let ticket = self.ticket;
        self.finish();
        state.order.wait_visible(&ticket);
    }

    /// End the claim with its rows in the index, leaving the wait for the mark to the caller
    pub fn finish(mut self) {
        self.state.order.finish(&self.ticket, TicketState::Landed);
        self.is_landed = true;
        self.release();
    }

    /// Let the segment seal once its last claim is done
    pub fn release(&self) {
        let mut claims = hold(&self.volume.claims);
        for segment in &mut claims.open {
            if segment.slot == self.slot {
                segment.in_flight -= 1;
                break;
            }
        }
        self.state.slot_files[self.slot.0 as usize].pins.fetch_sub(1, Ordering::AcqRel);
    }
}

impl<Backend: Io> Drop for Claim<'_, Backend> {
    fn drop(&mut self) {
        if !self.is_landed {
            // a failed writer must not hold the mark, or every later write would wait for it
            self.state.order.finish(&self.ticket, TicketState::Aborted);
            self.release();
        }
    }
}
