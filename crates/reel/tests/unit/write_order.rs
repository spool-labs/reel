use std::sync::Arc;

use reel::write_order::*;
use reel_api::units::{RowCount, WritePlace};

const ONE: RowCount = RowCount(1);
const NO_LIMIT: WritePlace = WritePlace(u64::MAX);

fn order_after(last: WritePlace) -> WriteOrder {
    WriteOrder::new(Arc::new(PlaceCount::new(last, NO_LIMIT)))
}

// claims take places in order, and a batch takes a run of them
#[test]
fn claim_order() {
    let order = order_after(WritePlace(10));

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
    let order = order_after(WritePlace(0));
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
    let order = order_after(WritePlace(0));
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
    let order = order_after(WritePlace(0));
    let batch = order.try_claim(RowCount(5)).expect("room");

    assert_eq!(order.mark(), WritePlace(0));
    order.finish(&batch, TicketState::Landed);
    assert_eq!(order.mark(), WritePlace(5));
}

// a full ring refuses a claim, and takes one again when the oldest ends
#[test]
fn ring_full() {
    let order = order_after(WritePlace(0));
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
    let order = order_after(WritePlace(0));
    for _claim in 0..MOST_IN_FLIGHT * 3 {
        let ticket = order.try_claim(ONE).expect("room");
        order.finish(&ticket, TicketState::Landed);
    }

    assert_eq!(order.mark(), WritePlace(MOST_IN_FLIGHT as u64 * 3));
}

// a claim that would pass the limit waits, and the places it would take stay free
#[test]
fn limit_stops() {
    let places = Arc::new(PlaceCount::new(WritePlace(0), WritePlace(2)));
    let order = WriteOrder::new(Arc::clone(&places));
    assert!(order.try_claim(RowCount(2)).is_some());
    assert!(places.is_past_limit(ONE));
    assert_eq!(order.try_claim(ONE), None);

    places.set_limit(WritePlace(3));

    assert!(!places.is_past_limit(ONE));
    assert_eq!(order.try_claim(ONE).expect("room").first, WritePlace(3));
}

// two volumes share one count, and each mark waits only for its own claims
#[test]
fn marks_stand_apart() {
    let places = Arc::new(PlaceCount::new(WritePlace(0), NO_LIMIT));
    let (bulk, meta) = (WriteOrder::new(Arc::clone(&places)), WriteOrder::new(Arc::clone(&places)));
    let slow = bulk.try_claim(ONE).expect("room");
    let quick = meta.try_claim(ONE).expect("room");
    assert_eq!((slow.first, quick.first), (WritePlace(1), WritePlace(2)));

    meta.finish(&quick, TicketState::Landed);
    assert_eq!(meta.mark(), quick.last);
    assert_eq!(bulk.mark(), WritePlace(0));
    meta.wait_visible(&quick);

    bulk.finish(&slow, TicketState::Landed);
    assert_eq!(bulk.mark(), slow.last);
}

// a mark moves past what recovery found, and never back
#[test]
fn resume_only_rises() {
    let order = order_after(WritePlace(10));
    order.resume_past(WritePlace(20));
    order.resume_past(WritePlace(15));
    assert_eq!(order.mark(), WritePlace(20));
}
