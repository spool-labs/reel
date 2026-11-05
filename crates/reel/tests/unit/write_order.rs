use reel::write_order::*;
use reel_api::units::{RowCount, WritePlace};

const ONE: RowCount = RowCount(1);
const NO_LIMIT: WritePlace = WritePlace(u64::MAX);

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
