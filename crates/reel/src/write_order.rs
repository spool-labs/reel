//! The store-wide count of places, and each volume's order and the mark that publishes it

use std::hint;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};

use reel_api::units::{RowCount, WritePlace};

use crate::guard::hold;

/// How many times a writer checks the mark before it sleeps: the writers ahead are usually a write call from done,
/// and a sleep costs both sides a system call
pub const MARK_SPINS: u32 = 256;
/// Writers that can be in flight at once on a volume
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
    waiters: u32,
}

/// The places a store hands out, one count across every volume, so a place means one thing everywhere
#[derive(Debug)]
pub struct PlaceCount {
    next: AtomicU64,
    limit: AtomicU64,
}

/// One volume's write order. A write waits only for this volume's earlier claims, so a slow disk holds back its own writes
#[derive(Debug)]
pub struct WriteOrder {
    places: Arc<PlaceCount>,
    ring: Mutex<TicketRing>,
    mark: AtomicU64,
    moved: Condvar,
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

impl PlaceCount {
    /// Start after the last place a store has used, with places reserved up to a limit
    pub fn new(last_used: WritePlace, limit: WritePlace) -> Self {
        Self {
            next: AtomicU64::new(last_used.0 + 1),
            limit: AtomicU64::new(limit.0),
        }
    }

    /// Read the last place handed out. A cue reads this before any mark, and that order makes its marks one moment
    pub fn last_given(&self) -> WritePlace {
        WritePlace(self.next.load(Ordering::Acquire) - 1)
    }

    /// Take the next places, or return nothing when the limit is in the way
    pub fn take(&self, rows: RowCount) -> Option<(WritePlace, WritePlace)> {
        let limit = self.limit.load(Ordering::Acquire);
        let taken = self.next.fetch_update(Ordering::AcqRel, Ordering::Acquire, |next| {
            let after = next + rows.0;
            (after - 1 <= limit).then_some(after)
        });
        let first = taken.ok()?;
        Some((WritePlace(first), WritePlace(first + rows.0 - 1)))
    }

    /// Check whether a claim of these rows would pass the limit
    pub fn is_past_limit(&self, rows: RowCount) -> bool {
        self.next.load(Ordering::Acquire) + rows.0 - 1 > self.limit.load(Ordering::Acquire)
    }

    /// Move the count past a place recovery found
    pub fn resume_past(&self, place: WritePlace) {
        self.next.fetch_max(place.0 + 1, Ordering::AcqRel);
    }

    /// Count the places left under the limit
    pub fn left(&self) -> u64 {
        self.limit.load(Ordering::Acquire).saturating_sub(self.next.load(Ordering::Acquire))
    }

    /// Set the limit, once every root holds it
    pub fn set_limit(&self, limit: WritePlace) {
        self.limit.store(limit.0, Ordering::Release);
    }
}

impl WriteOrder {
    /// Start an order with nothing in flight, on the store's count
    pub fn new(places: Arc<PlaceCount>) -> Self {
        let mark = places.last_given();
        Self {
            places,
            ring: Mutex::new(TicketRing {
                states: vec![TicketState::Landed; MOST_IN_FLIGHT],
                ends: vec![WritePlace(0); MOST_IN_FLIGHT],
                head: 0,
                tail: 0,
                waiters: 0,
            }),
            mark: AtomicU64::new(mark.0),
            moved: Condvar::new(),
            wakes: AtomicU64::new(0),
        }
    }

    /// Count the wake-ups so far, each a system call
    pub fn wakes(&self) -> u64 {
        self.wakes.load(Ordering::Relaxed)
    }

    /// Read the mark: every place of this volume at or below it is published
    pub fn mark(&self) -> WritePlace {
        WritePlace(self.mark.load(Ordering::Acquire))
    }

    /// Claim the next places, or return nothing while too many writers are in flight or the limit is in the way
    pub fn try_claim(&self, rows: RowCount) -> Option<Ticket> {
        let mut ring = hold(&self.ring);
        if ring.len() == MOST_IN_FLIGHT {
            return None;
        }

        // the count moves under this lock, so this volume's claims stand in the ring in place order
        let (first, last) = self.places.take(rows)?;
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
            // a notify makes a system call whoever listens, so it waits for someone to wake
            if ring.waiters > 0 {
                self.wakes.fetch_add(1, Ordering::Relaxed);
                self.moved.notify_all();
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

        let mut ring = hold(&self.ring);
        ring.waiters += 1;
        while self.mark().0 < ticket.last.0 {
            ring = match self.moved.wait(ring) {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
        }
        ring.waiters -= 1;
    }

    /// Move the mark past a place recovery found, with no claim in flight
    pub fn resume_past(&self, place: WritePlace) {
        self.mark.fetch_max(place.0, Ordering::AcqRel);
    }
}
