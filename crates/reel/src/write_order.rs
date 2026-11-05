//! The single store-wide order, and the mark that publishes it

use std::hint;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::thread::{self, Thread};

use reel_api::units::{RowCount, WritePlace};

use crate::guard::hold;

/// How many times a writer checks the mark before it sleeps: the writers ahead are usually a write call from done,
/// and a sleep costs both sides a system call
pub const MARK_SPINS: u32 = 256;
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

/// Where a ticket's writer sleeps. `waiting` is the place it waits for, zero when nobody sleeps here. A slot serves a
/// new ticket each time the ring comes round, and the place says which one it's for
#[derive(Debug, Default)]
pub struct ParkSlot {
    waiting: AtomicU64,
    thread: Mutex<Option<Thread>>,
}

/// The write order of a store
#[derive(Debug)]
pub struct WriteOrder {
    ring: Mutex<TicketRing>,
    mark: AtomicU64,
    parks: Vec<ParkSlot>,
    wakes: AtomicU64,
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
            parks: (0..MOST_IN_FLIGHT).map(|_slot| ParkSlot::default()).collect(),
            wakes: AtomicU64::new(0),
        }
    }

    /// Count the wake-ups so far, each a system call
    pub fn wakes(&self) -> u64 {
        self.wakes.load(Ordering::Relaxed)
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

    /// End a claim, move the mark past every finished claim in order, and wake only the writers it passed
    pub fn finish(&self, ticket: &Ticket, state: TicketState) {
        let (first, end, highest) = {
            let mut ring = hold(&self.ring);
            ring.states[ticket.slot] = state;

            let first = ring.head;
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
                // a sleeper writes its place and then reads the mark, so this store and the reads below meet it
                self.mark.store(place.0, Ordering::SeqCst);
            }
            (first, ring.head, highest)
        };

        let Some(place) = highest else {
            return;
        };
        for index in first..end {
            let park = &self.parks[index % MOST_IN_FLIGHT];
            let waiting = park.waiting.load(Ordering::SeqCst);
            if waiting != 0
                && waiting <= place.0
                && let Some(thread) = hold(&park.thread).as_ref()
            {
                self.wakes.fetch_add(1, Ordering::Relaxed);
                thread.unpark();
            }
        }
    }

    /// Wait until the mark passes a claim, so the write is visible to every later read
    pub fn wait_visible(&self, ticket: &Ticket) {
        for _spin in 0..MARK_SPINS {
            if self.mark().0 >= ticket.last.0 {
                return;
            }
            hint::spin_loop();
        }

        let park = &self.parks[ticket.slot];
        loop {
            if self.mark().0 >= ticket.last.0 {
                return;
            }
            if self.take_park(park, ticket) {
                break;
            }
            // a writer the ring lapped is still leaving this slot, and the mark has passed it
            thread::yield_now();
        }
        while self.mark.load(Ordering::SeqCst) < ticket.last.0 {
            thread::park();
        }
        park.waiting.store(0, Ordering::SeqCst);
    }

    /// Find a ticket's park slot
    pub fn park_of(&self, ticket: &Ticket) -> &ParkSlot {
        &self.parks[ticket.slot]
    }

    /// Read the next place a claim would take
    pub fn next_place(&self) -> WritePlace {
        hold(&self.ring).next_place
    }

    /// Check a writer sleeps in a ticket's slot
    pub fn has_sleeper(&self, ticket: &Ticket) -> bool {
        self.parks[ticket.slot].waiting.load(Ordering::SeqCst) == ticket.last.0
    }

    /// Take a ticket's park slot, or return false while an earlier writer still has it
    pub fn take_park(&self, park: &ParkSlot, ticket: &Ticket) -> bool {
        let mut thread = hold(&park.thread);
        if park.waiting.load(Ordering::SeqCst) != 0 {
            return false;
        }
        *thread = Some(thread::current());
        park.waiting.store(ticket.last.0, Ordering::SeqCst);
        true
    }

    /// Check whether a claim of these rows would pass the limit
    pub fn is_past_limit(&self, rows: RowCount) -> bool {
        let ring = hold(&self.ring);
        ring.next_place.0 + rows.0 - 1 > ring.limit.0
    }

    /// Move the order past a place recovery found
    pub fn resume_past(&self, place: WritePlace) {
        let mut ring = hold(&self.ring);
        if place >= ring.next_place {
            ring.next_place = WritePlace(place.0 + 1);
            self.mark.store(place.0, Ordering::Release);
        }
    }

    /// Count the places left under the limit
    pub fn places_left(&self) -> u64 {
        let ring = hold(&self.ring);
        ring.limit.0.saturating_sub(ring.next_place.0)
    }

    /// Set the limit, once every root holds it
    pub fn set_limit(&self, limit: WritePlace) {
        hold(&self.ring).limit = limit;
    }
}
