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
    /// Start the order after the last place a store has used
    pub fn new(last_used: WritePlace) -> Self {
        Self {
            ring: Mutex::new(TicketRing {
                states: vec![TicketState::Landed; MOST_IN_FLIGHT],
                ends: vec![WritePlace(0); MOST_IN_FLIGHT],
                head: 0,
                tail: 0,
                next_place: WritePlace(last_used.0 + 1),
            }),
            mark: AtomicU64::new(last_used.0),
            moved: Condvar::new(),
        }
    }

    /// Read the mark: every place at or below it is published
    pub fn mark(&self) -> WritePlace {
        WritePlace(self.mark.load(Ordering::Acquire))
    }

    /// Claim the next places, or return nothing while too many writers are in flight
    pub fn try_claim(&self, rows: RowCount) -> Option<Ticket> {
        let mut ring = hold(&self.ring);
        if ring.len() == MOST_IN_FLIGHT {
            return None;
        }

        let first = ring.next_place;
        let last = WritePlace(first.0 + rows.0 - 1);
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

    /// Move the order past the highest place an open found
    pub fn resume_after(&self, last_used: WritePlace) {
        hold(&self.ring).next_place = WritePlace(last_used.0 + 1);
        self.mark.store(last_used.0, Ordering::Release);
    }

    /// Read the next place a claim would take
    pub fn next_place(&self) -> WritePlace {
        hold(&self.ring).next_place
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ONE: RowCount = RowCount(1);

    // claims take places in order, and a batch takes a run of them
    #[test]
    fn claim_order() {
        let order = WriteOrder::new(WritePlace(10));

        let first = order.try_claim(ONE).expect("room");
        let batch = order.try_claim(RowCount(3)).expect("room");
        let last = order.try_claim(ONE).expect("room");

        assert_eq!((first.first, first.last), (WritePlace(11), WritePlace(11)));
        assert_eq!((batch.first, batch.last), (WritePlace(12), WritePlace(14)));
        assert_eq!(last.first, WritePlace(15));
    }

    // the mark waits for the oldest claim, then jumps over every finished one
    #[test]
    fn mark_order() {
        let order = WriteOrder::new(WritePlace(0));
        let first = order.try_claim(ONE).expect("room");
        let second = order.try_claim(ONE).expect("room");
        let third = order.try_claim(ONE).expect("room");

        order.finish(&second, TicketState::Landed);
        order.finish(&third, TicketState::Landed);
        assert_eq!(order.mark(), WritePlace(0));

        order.finish(&first, TicketState::Landed);
        assert_eq!(order.mark(), third.last);
    }

    // an aborted claim lets the mark pass it
    #[test]
    fn mark_abort() {
        let order = WriteOrder::new(WritePlace(0));
        let first = order.try_claim(ONE).expect("room");
        let second = order.try_claim(ONE).expect("room");

        order.finish(&first, TicketState::Aborted);
        assert_eq!(order.mark(), first.last);
        order.finish(&second, TicketState::Landed);
        assert_eq!(order.mark(), second.last);
    }

    // a batch moves the mark in one step, so it's seen whole or not at all
    #[test]
    fn mark_batch() {
        let order = WriteOrder::new(WritePlace(0));
        let batch = order.try_claim(RowCount(5)).expect("room");

        assert_eq!(order.mark(), WritePlace(0));
        order.finish(&batch, TicketState::Landed);
        assert_eq!(order.mark(), WritePlace(5));
    }

    // a full ring refuses a claim, and takes one again when the oldest ends
    #[test]
    fn ring_full() {
        let order = WriteOrder::new(WritePlace(0));
        let first = order.try_claim(ONE).expect("room");
        for _claim in 1..MOST_IN_FLIGHT {
            assert!(order.try_claim(ONE).is_some());
        }

        assert_eq!(order.try_claim(ONE), None);
        order.finish(&first, TicketState::Landed);
        assert!(order.try_claim(ONE).is_some());
    }

    // the ring reuses its slots past its size
    #[test]
    fn ring_reuse() {
        let order = WriteOrder::new(WritePlace(0));
        for _claim in 0..MOST_IN_FLIGHT * 3 {
            let ticket = order.try_claim(ONE).expect("room");
            order.finish(&ticket, TicketState::Landed);
        }

        assert_eq!(order.mark(), WritePlace(MOST_IN_FLIGHT as u64 * 3));
    }
}
