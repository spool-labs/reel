use std::thread;

use reel::write_order::*;
use reel_api::units::{RowCount, WritePlace};

const ONE: RowCount = RowCount(1);
const NO_LIMIT: WritePlace = WritePlace(u64::MAX);
const WRITERS: u64 = 8;
const CLAIMS: u64 = 20_000;

// claims take places in order, and a batch takes a run of them
#[test]
fn claim_order() {
    let order = WriteOrder::new(WritePlace(10), NO_LIMIT);

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
    let order = WriteOrder::new(WritePlace(0), NO_LIMIT);
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
    let order = WriteOrder::new(WritePlace(0), NO_LIMIT);
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
    let order = WriteOrder::new(WritePlace(0), NO_LIMIT);
    let batch = order.try_claim(RowCount(5)).expect("room");

    assert_eq!(order.mark(), WritePlace(0));
    order.finish(&batch, TicketState::Landed);
    assert_eq!(order.mark(), WritePlace(5));
}

// a full ring refuses a claim, and takes one again when the oldest ends
#[test]
fn ring_full() {
    let order = WriteOrder::new(WritePlace(0), NO_LIMIT);
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
    let order = WriteOrder::new(WritePlace(0), NO_LIMIT);
    for _claim in 0..MOST_IN_FLIGHT * 3 {
        let ticket = order.try_claim(ONE).expect("room");
        order.finish(&ticket, TicketState::Landed);
    }

    assert_eq!(order.mark(), WritePlace(MOST_IN_FLIGHT as u64 * 3));
}

// a claim that would pass the limit waits, and the places it would take stay free
#[test]
fn limit_stops() {
    let order = WriteOrder::new(WritePlace(0), WritePlace(2));
    assert!(order.try_claim(RowCount(2)).is_some());
    assert!(order.is_past_limit(ONE));
    assert_eq!(order.try_claim(ONE), None);

    order.set_limit(WritePlace(3));

    assert!(!order.is_past_limit(ONE));
    assert_eq!(order.try_claim(ONE).expect("room").first, WritePlace(3));
}

fn wait_for_sleeper(order: &WriteOrder, ticket: &Ticket) {
    while !order.has_sleeper(ticket) {
        thread::yield_now();
    }
}

// a sleeping writer wakes once when the mark passes it
#[test]
fn sleeper_wakes() {
    let order = WriteOrder::new(WritePlace(0), NO_LIMIT);
    let first = order.try_claim(ONE).expect("room");
    let second = order.try_claim(ONE).expect("room");
    order.finish(&second, TicketState::Landed);

    thread::scope(|scope| {
        let waiter = scope.spawn(|| order.wait_visible(&second));
        wait_for_sleeper(&order, &second);
        assert_eq!(order.wakes(), 0);
        order.finish(&first, TicketState::Landed);
        waiter.join().expect("join");
    });

    assert_eq!(order.wakes(), 1);
    assert!(!order.has_sleeper(&second));
}

// a move of the mark wakes only the writers it passed
#[test]
fn wakes_passed_only() {
    let order = WriteOrder::new(WritePlace(0), NO_LIMIT);
    let tickets: Vec<_> = (0..4).map(|_ticket| order.try_claim(ONE).expect("room")).collect();
    order.finish(&tickets[1], TicketState::Landed);
    order.finish(&tickets[3], TicketState::Landed);

    thread::scope(|scope| {
        let near = scope.spawn(|| order.wait_visible(&tickets[1]));
        let far = scope.spawn(|| order.wait_visible(&tickets[3]));
        wait_for_sleeper(&order, &tickets[1]);
        wait_for_sleeper(&order, &tickets[3]);

        order.finish(&tickets[0], TicketState::Landed);
        near.join().expect("join");
        assert_eq!(order.wakes(), 1);
        assert!(order.has_sleeper(&tickets[3]));

        order.finish(&tickets[2], TicketState::Landed);
        far.join().expect("join");
    });

    assert_eq!(order.wakes(), 2);
}

// a ticket the ring brings back to a slot waits for an earlier writer to leave it
#[test]
fn lapped_slot() {
    let order = WriteOrder::new(WritePlace(0), NO_LIMIT);
    let first = order.try_claim(ONE).expect("room");
    let lapped = Ticket {
        first: WritePlace(5_000),
        last: WritePlace(5_000),
        slot: first.slot,
    };
    let park = order.park_of(&first);

    assert!(order.take_park(park, &first));
    assert!(!order.take_park(park, &lapped));
    assert!(order.has_sleeper(&first));
    assert!(!order.has_sleeper(&lapped));
}

// writers that land and wait on many threads never hang, and the mark ends past every claim
#[test]
fn many_writers() {
    let order = WriteOrder::new(WritePlace(0), NO_LIMIT);
    thread::scope(|scope| {
        for _writer in 0..WRITERS {
            scope.spawn(|| {
                for _claim in 0..CLAIMS {
                    let Some(ticket) = order.try_claim(ONE) else {
                        continue;
                    };
                    order.finish(&ticket, TicketState::Landed);
                    order.wait_visible(&ticket);
                }
            });
        }
    });

    assert_eq!(order.mark().0, order.next_place().0 - 1);
}
