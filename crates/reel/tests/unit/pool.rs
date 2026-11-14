use std::panic::{self, AssertUnwindSafe};
use std::thread;
use std::time::Duration;

use reel::pool::*;
use reel_sim::wait::wait_until;

// an item goes back after use, also when the work panics
#[test]
fn item_returns() {
    let pool = Pool::new(vec![1_u32]);

    assert_eq!(pool.with(|item| *item + 1), 2);
    let outcome = panic::catch_unwind(AssertUnwindSafe(|| pool.with(|_item| panic!("failed work"))));

    assert!(outcome.is_err());
    assert_eq!(pool.with(|item| *item), 1);
}

// a caller in the queue wakes for an item handed back, and a running caller may take one ahead of it while it sleeps
#[test]
fn queued_caller_gets_through() {
    let pool = Pool::new(vec![0_u32]);
    let item = pool.take();

    thread::scope(|scope| {
        let waiter = scope.spawn(|| pool.with(|item| *item));
        wait_until(|| pool.waiting() > 0, "no taker waited");

        pool.give_back(item);
        assert_eq!(waiter.join().expect("join"), 0);
    });
    assert_eq!(pool.waiting(), 0);
    assert_eq!(pool.free_count(), 1);
}

// several callers in line each get an item, also when more than one comes free at once
#[test]
fn queue_drains() {
    let pool = Pool::new(vec![0_u32, 1]);
    let first = pool.take();
    let second = pool.take();

    thread::scope(|scope| {
        let waiters: Vec<_> = (0..4).map(|_thread| scope.spawn(|| pool.with(|item| *item))).collect();
        wait_until(|| pool.waiting() >= 4, "four takers never waited");

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
        wait_until(|| pool.waiting() > 0, "no taker waited");

        pool.give_back(first);
        pool.give_back(second);

        assert_eq!(waiter.join().expect("join"), 3);
    });
    assert_eq!(pool.waiting(), 0);
}

// real threads taking one and many items through a pool too small for them lose no wake and no item
#[test]
fn threads_lose_nothing() {
    const ITEMS: usize = 3;
    const THREADS: usize = 8;
    const ROUNDS: usize = 2_000;
    let pool = Pool::new((0..ITEMS).collect::<Vec<_>>());
    thread::scope(|scope| {
        for thread_index in 0..THREADS {
            let pool = &pool;
            scope.spawn(move || {
                let mut items = Vec::with_capacity(2);
                for round in 0..ROUNDS {
                    if (thread_index + round) % 5 == 0 {
                        pool.take_many(2, &mut items);
                        assert_eq!(items.len(), 2);
                        pool.give_back_all(&mut items);
                    } else {
                        let item = pool.take();
                        pool.give_back(item);
                    }
                }
            });
        }
    });
    let mut left = Vec::new();
    pool.take_many(ITEMS, &mut left);
    left.sort_unstable();
    assert_eq!(left, (0..ITEMS).collect::<Vec<_>>());
}

// many more threads than items, each holding its item a moment so a line forms, all get through and lose no item
#[test]
fn crowd_wakes_its_own() {
    const ITEMS: usize = 8;
    const THREADS: usize = 64;
    const ROUNDS: usize = 200;
    const HOLD_MICROS: u64 = 20;
    let pool = Pool::new((0..ITEMS).collect::<Vec<_>>());
    thread::scope(|scope| {
        for _thread in 0..THREADS {
            let pool = &pool;
            scope.spawn(move || {
                for _round in 0..ROUNDS {
                    let item = pool.take();
                    thread::sleep(Duration::from_micros(HOLD_MICROS));
                    pool.give_back(item);
                }
            });
        }
    });
    let (waits, wakes) = (pool.waits(), pool.wakes());
    println!("{waits} waits, {wakes} wakes");
    assert_eq!(pool.free_count(), ITEMS);
}

// a caller who needs four waits behind single takers and gets all four
#[test]
fn many_behind_singles() {
    const ITEMS: usize = 4;
    let pool = Pool::new((0..ITEMS).collect::<Vec<_>>());
    let held: Vec<_> = (0..ITEMS).map(|_item| pool.take()).collect();
    thread::scope(|scope| {
        let singles: Vec<_> = (0..3).map(|_thread| scope.spawn(|| pool.with(|item| *item))).collect();
        wait_until(|| pool.waiting() >= 3, "the single takers never waited");
        let many = scope.spawn(|| {
            let mut items = Vec::with_capacity(ITEMS);
            pool.take_many(ITEMS, &mut items);
            let count = items.len();
            pool.give_back_all(&mut items);
            count
        });
        wait_until(|| pool.waiting() >= 4, "the many taker never waited");
        for item in held {
            pool.give_back(item);
        }
        for single in singles {
            assert!(single.join().expect("join") < ITEMS);
        }
        assert_eq!(many.join().expect("join"), ITEMS);
    });
    assert_eq!(pool.waiting(), 0);
}

// more waiters than wake lines share lines and still all get through
#[test]
fn waiters_past_the_lines() {
    let pool = Pool::new(vec![0_u32]);
    let item = pool.take();
    let waiters = WAKE_LINES + 8;
    thread::scope(|scope| {
        let threads: Vec<_> = (0..waiters).map(|_thread| scope.spawn(|| pool.with(|item| *item))).collect();
        wait_until(|| pool.waiting() >= waiters as u64, "the waiters never all queued");
        pool.give_back(item);
        for thread in threads {
            assert_eq!(thread.join().expect("join"), 0);
        }
    });
    assert_eq!(pool.waiting(), 0);
}

// a caller asleep in line gets an item soon, while another caller takes and gives one back with no pause
#[test]
fn sleeper_is_not_passed_over() {
    const RUN: Duration = Duration::from_secs(1);
    const MOST_WAIT: Duration = Duration::from_millis(50);
    const HOLD: Duration = Duration::from_micros(50);
    let pool = Pool::new(vec![0_u32]);
    let is_done = std::sync::atomic::AtomicBool::new(false);
    thread::scope(|scope| {
        scope.spawn(|| {
            let start = std::time::Instant::now();
            while start.elapsed() < RUN && !is_done.load(std::sync::atomic::Ordering::Relaxed) {
                let item = pool.take();
                // a busy reader holds its item a while, so the item is free only between its give and its next take
                let held = std::time::Instant::now();
                while held.elapsed() < HOLD {
                    std::hint::spin_loop();
                }
                pool.give_back(item);
            }
        });
        thread::sleep(Duration::from_millis(10));
        let start = std::time::Instant::now();
        let item = pool.take();
        let waited = start.elapsed();
        pool.give_back(item);
        is_done.store(true, std::sync::atomic::Ordering::Relaxed);
        assert!(waited < MOST_WAIT, "the sleeper waited {waited:?} behind a busy caller");
        assert!(pool.passes() <= 1, "the sleeper was passed over {} times", pool.passes());
    });
}
