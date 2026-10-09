//! Many parking and async waiters on one waitlist, each test behind a deadline

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Barrier};
use std::task::{Context, Poll, Wake, Waker};
use std::thread;
use std::time::Duration;

use reel::append::admission::InflightBudget;
use reel::{ByteCount, Tension};

/// A run that takes longer than this counts as hung
const DEADLINE: Duration = Duration::from_secs(60);

/// Threads per shape, so a run has this many parking and this many holding futures
const PER_SHAPE: usize = 8;

/// Each thread runs this many rounds
const ROUNDS: usize = 2_000;

/// The pool holds this many permits, well under what its threads want at once
const PERMITS: u64 = 4;

/// Drive one future to its answer on the calling thread
fn block_on<Answered: Future>(future: Answered) -> Answered::Output {
    struct Unparker(thread::Thread);

    impl Wake for Unparker {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.unpark();
        }
    }

    let mut future = Box::pin(future);
    let waker = Waker::from(Arc::new(Unparker(thread::current())));
    let mut cx = Context::from_waker(&waker);
    loop {
        if let Poll::Ready(answer) = future.as_mut().poll(&mut cx) {
            return answer;
        }
        thread::park();
    }
}

/// Join the workers behind a deadline, so a lost wakeup fails the test
fn within_deadline(workers: Vec<thread::JoinHandle<()>>, doing: &str) {
    let (done, finished) = mpsc::channel();
    thread::spawn(move || {
        for worker in workers {
            worker.join().expect("worker joins");
        }
        let _ = done.send(());
    });
    finished
        .recv_timeout(DEADLINE)
        .unwrap_or_else(|_| panic!("{doing} did not finish, so a waiter was never woken"));
}

/// Bytes one thread asks for in one round, unlike its neighbour's ask
fn ask(thread: usize, round: usize) -> u64 {
    let steps = [512u64, 1024, 1536, 2048, 3072];
    steps[(thread + round) % steps.len()]
}

// every writer on a bounded budget gets through, in whichever shape it took
#[test]
fn a_budget_admits_both_shapes_under_contention() {
    let budget = Arc::new(InflightBudget::new(ByteCount::from_bytes(4096)));
    let start = Arc::new(Barrier::new(PER_SHAPE * 2));
    let admitted = Arc::new(AtomicU64::new(0));

    let mut workers = Vec::new();
    for thread_index in 0..PER_SHAPE {
        let budget = Arc::clone(&budget);
        let start = Arc::clone(&start);
        let admitted = Arc::clone(&admitted);
        workers.push(thread::spawn(move || {
            start.wait();
            for round in 0..ROUNDS {
                let bytes = ask(thread_index, round);
                budget.acquire(bytes);
                admitted.fetch_add(bytes, Ordering::Relaxed);
                thread::yield_now();
                budget.release(bytes);
            }
        }));
    }
    for thread_index in 0..PER_SHAPE {
        let budget = Arc::clone(&budget);
        let start = Arc::clone(&start);
        let admitted = Arc::clone(&admitted);
        workers.push(thread::spawn(move || {
            start.wait();
            for round in 0..ROUNDS {
                let bytes = ask(thread_index + 1, round);
                block_on(budget.reserve(bytes));
                admitted.fetch_add(bytes, Ordering::Relaxed);
                thread::yield_now();
                budget.release(bytes);
            }
        }));
    }

    within_deadline(workers, "a budget under contention");

    assert_eq!(
        budget.queued_bytes(),
        ByteCount::from_bytes(0),
        "every admitted byte was released"
    );
    assert!(admitted.load(Ordering::Relaxed) > 0);
}

// a pool of permits comes back whole after both shapes have taken from it
#[test]
fn a_waitlist_hands_out_every_permit() {
    let pool = Arc::new(Tension::new(PERMITS));
    let start = Arc::new(Barrier::new(PER_SHAPE * 2));

    // Count unmet conditions, since a run with none never contended
    let blocked = Arc::new(AtomicU64::new(0));

    let mut workers = Vec::new();
    for thread_index in 0..PER_SHAPE * 2 {
        let pool = Arc::clone(&pool);
        let start = Arc::clone(&start);
        let blocked = Arc::clone(&blocked);
        let is_parking = thread_index % 2 == 0;
        workers.push(thread::spawn(move || {
            start.wait();
            for round in 0..ROUNDS {
                // Ask for one to three permits, so a release can wake a waiter that is still short
                let wanted = 1 + ((thread_index + round) % 3) as u64;
                let take = |held: &mut u64| {
                    if *held < wanted {
                        blocked.fetch_add(1, Ordering::Relaxed);
                        return None;
                    }
                    *held -= wanted;
                    Some(())
                };
                if is_parking {
                    pool.park(take);
                } else {
                    block_on(pool.wait(take));
                }
                // Hold the permits across a yield, so the threads behind ask while they are out
                thread::yield_now();
                pool.slack_with(|held| *held += wanted);
            }
        }));
    }

    within_deadline(workers, "a waitlist under contention");

    pool.with(|held| assert_eq!(*held, PERMITS, "every permit came back"));
    assert!(!pool.is_taut(), "nothing is left waiting");
    assert!(
        blocked.load(Ordering::Relaxed) > 0,
        "the run never contended, so it checked nothing"
    );
}

// a wait abandoned while pending leaves the list as it found it
#[test]
fn abandoned_waits_leave_no_seats() {
    let pool = Arc::new(Tension::new(0u64));
    let start = Arc::new(Barrier::new(PER_SHAPE + 1));

    let mut workers = Vec::new();
    for _ in 0..PER_SHAPE {
        let pool = Arc::clone(&pool);
        let start = Arc::clone(&start);
        workers.push(thread::spawn(move || {
            start.wait();
            for _ in 0..ROUNDS {
                // One poll takes a seat, and the drop abandons it while the list is being woken
                let mut abandoned = Box::pin(pool.wait(|held: &mut u64| (*held > 0).then_some(())));
                let _ = Pin::as_mut(&mut abandoned).poll(&mut Context::from_waker(Waker::noop()));
                drop(abandoned);
            }
        }));
    }

    let waker = Arc::clone(&pool);
    let start_waking = Arc::clone(&start);
    workers.push(thread::spawn(move || {
        start_waking.wait();
        for _ in 0..ROUNDS * PER_SHAPE {
            waker.slack();
        }
    }));

    within_deadline(workers, "abandoned waits");

    assert!(
        !pool.is_taut(),
        "every abandoned wait took its seat with it"
    );
}

// a wait that has answered takes nothing more, however often it is polled
#[test]
fn a_spent_wait_takes_once() {
    let pool = Tension::new(4u64);
    let mut taken = 0u64;

    {
        let mut wait = Box::pin(pool.wait(|held: &mut u64| {
            if *held == 0 {
                return None;
            }
            *held -= 2;
            taken += 2;
            Some(())
        }));
        let mut cx = Context::from_waker(Waker::noop());
        assert!(Pin::as_mut(&mut wait).poll(&mut cx).is_ready());
        for _ in 0..4 {
            assert!(
                Pin::as_mut(&mut wait).poll(&mut cx).is_pending(),
                "a spent wait never asks its condition again"
            );
        }
    }

    assert_eq!(taken, 2, "the permit was taken once");
    pool.with(|held| assert_eq!(*held, 2, "and the pool was charged once"));
}
