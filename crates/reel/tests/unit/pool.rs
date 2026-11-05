use std::panic::{self, AssertUnwindSafe};
use std::sync::atomic::{AtomicU32, Ordering};
use std::thread;

use reel::pool::*;

const FIRST: u32 = 1;
const SECOND: u32 = 2;

// an item goes back after use, also when the work panics
#[test]
fn item_returns() {
    let pool = Pool::new(vec![1_u32]);

    assert_eq!(pool.with(|item| *item + 1), 2);
    let outcome = panic::catch_unwind(AssertUnwindSafe(|| pool.with(|_item| panic!("failed work"))));

    assert!(outcome.is_err());
    assert_eq!(pool.with(|item| *item), 1);
}

// a caller in the queue gets the next free item, ahead of the one that just gave it back
#[test]
fn first_come() {
    let pool = Pool::new(vec![0_u32]);
    let order = AtomicU32::new(0);
    let item = pool.take();

    thread::scope(|scope| {
        let waiter = scope.spawn(|| {
            pool.with(|_item| order.compare_exchange(0, FIRST, Ordering::AcqRel, Ordering::Acquire).is_ok())
        });
        while pool.waiting() == 0 {
            thread::yield_now();
        }

        pool.give_back(item);
        let is_second = pool.with(|_item| order.compare_exchange(FIRST, SECOND, Ordering::AcqRel, Ordering::Acquire).is_ok());

        assert!(is_second);
        assert!(waiter.join().expect("join"));
    });
}

// several callers in line each get an item, also when more than one comes free at once
#[test]
fn queue_drains() {
    let pool = Pool::new(vec![0_u32, 1]);
    let first = pool.take();
    let second = pool.take();

    thread::scope(|scope| {
        let waiters: Vec<_> = (0..4).map(|_thread| scope.spawn(|| pool.with(|item| *item))).collect();
        while pool.waiting() < 4 {
            thread::yield_now();
        }

        pool.give_back(first);
        pool.give_back(second);

        for waiter in waiters {
            assert!(waiter.join().expect("join") < 2);
        }
    });
    assert_eq!(pool.waiting(), 0);
}

// a caller who needs several waits in line without holding any, and gets them all at once
#[test]
fn many_at_once() {
    let pool = Pool::new(vec![0_u32, 1, 2]);
    let first = pool.take();
    let second = pool.take();

    thread::scope(|scope| {
        let waiter = scope.spawn(|| {
            let mut items = Vec::with_capacity(3);
            pool.take_many(3, &mut items);
            let count = items.len();
            pool.give_back_all(&mut items);
            count
        });
        while pool.waiting() == 0 {
            thread::yield_now();
        }

        pool.give_back(first);
        pool.give_back(second);

        assert_eq!(waiter.join().expect("join"), 3);
    });
    assert_eq!(pool.waiting(), 0);
}
