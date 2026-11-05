//! The single store-wide order, and the mark that publishes it

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Condvar, Mutex};

use reel_api::units::{RowCount, WritePlace};

use crate::guard::hold;

/// Writers that can be in flight at once
pub const MOST_IN_FLIGHT: usize = 1_024;

/// How one claim ended
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TicketState {
    /// Its writer is still working, and the mark waits for it
    InFlight,
    /// Its rows are in the index
    Landed,
    /// Its writer failed, so the mark passes it with nothing to show
    Aborted,
}

/// One claim of places, in write order
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Ticket {
    pub first: WritePlace,
    pub last: WritePlace,
    pub slot: usize,
}

/// The claims still ahead of the mark, oldest first
#[derive(Debug)]
pub struct TicketRing {
    states: Vec<TicketState>,
    ends: Vec<WritePlace>,
    head: usize,
    tail: usize,
    next_place: WritePlace,
    limit: WritePlace,
}

/// The write order of a store
#[derive(Debug)]
pub struct WriteOrder {
    ring: Mutex<TicketRing>,
    mark: AtomicU64,
    moved: Condvar,
}

impl TicketRing {
    /// Count the claims in flight
    pub fn len(&self) -> usize {
        self.tail - self.head
    }

    /// Check whether no claim is in flight
    pub fn is_empty(&self) -> bool {
        self.head == self.tail
    }
}

impl WriteOrder {
    /// Start the order after the last place a store has used, with places reserved up to a limit
    pub fn new(last_used: WritePlace, limit: WritePlace) -> Self {
        Self {
            ring: Mutex::new(TicketRing {
                states: vec![TicketState::Landed; MOST_IN_FLIGHT],
                ends: vec![WritePlace(0); MOST_IN_FLIGHT],
                head: 0,
                tail: 0,
                next_place: WritePlace(last_used.0 + 1),
                limit,
            }),
            mark: AtomicU64::new(last_used.0),
            moved: Condvar::new(),
        }
    }

    /// Read the mark: every place at or below it is published
    pub fn mark(&self) -> WritePlace {
        WritePlace(self.mark.load(Ordering::Acquire))
    }

    /// Claim the next places, or return nothing while too many writers are in flight or the limit is in the way
    pub fn try_claim(&self, rows: RowCount) -> Option<Ticket> {
        let mut ring = hold(&self.ring);
        if ring.len() == MOST_IN_FLIGHT {
            return None;
        }

        let first = ring.next_place;
        let last = WritePlace(first.0 + rows.0 - 1);
        if last > ring.limit {
            return None;
        }
        ring.next_place = WritePlace(last.0 + 1);

        let slot = ring.tail % MOST_IN_FLIGHT;
        ring.states[slot] = TicketState::InFlight;
        ring.ends[slot] = last;
        ring.tail += 1;
        Some(Ticket { first, last, slot })
    }

    /// End a claim, and move the mark past every finished claim in order
    pub fn finish(&self, ticket: &Ticket, state: TicketState) {
        let mut ring = hold(&self.ring);
        ring.states[ticket.slot] = state;

        let mut highest = None;
        while !ring.is_empty() {
            let head = ring.head % MOST_IN_FLIGHT;
            if ring.states[head] == TicketState::InFlight {
                break;
            }
            highest = Some(ring.ends[head]);
            ring.head += 1;
        }

        if let Some(place) = highest {
            self.mark.store(place.0, Ordering::Release);
            self.moved.notify_all();
        }
    }

    /// Wait until the mark passes a claim, so the write is visible to every later read
    pub fn wait_visible(&self, ticket: &Ticket) {
        if self.mark().0 >= ticket.last.0 {
            return;
        }

        let mut ring = hold(&self.ring);
        while self.mark().0 < ticket.last.0 {
            ring = match self.moved.wait(ring) {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
        }
    }

    /// Check whether a claim of these rows would pass the limit
    pub fn is_past_limit(&self, rows: RowCount) -> bool {
        let ring = hold(&self.ring);
        ring.next_place.0 + rows.0 - 1 > ring.limit.0
    }

    /// Set the limit, once every root holds it
    pub fn set_limit(&self, limit: WritePlace) {
        hold(&self.ring).limit = limit;
    }
}
