//! The barrier that makes a batch's index moves land together for multi-key reads

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, PoisonError};
use std::thread::{self, Thread};

use crate::sync::lock;

/// What one waiter is asking for
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Want {
    /// A read resolving several keys, which may share with other reads
    Shared,

    /// A batch moving the index, which may share with nothing
    Exclusive,
}

/// One caller waiting its turn, and the thread to hand that turn to
#[derive(Debug)]
struct Waiter {
    /// The place it took in arrival order
    place: u64,

    /// What it is waiting to do
    want: Want,

    /// The thread to unpark once that place reaches the front
    thread: Thread,
}

/// Who is inside and who is waiting, in arrival order
#[derive(Debug, Default)]
struct Queue {
    /// Reads currently inside
    readers: usize,

    /// Whether a batch is currently inside
    is_publishing: bool,

    /// What each waiter wants and the place it took, oldest first
    waiting: VecDeque<Waiter>,

    /// The next place to hand out
    next_place: u64,
}

impl Queue {
    /// Whether this waiter is at the front and what it wants is free
    fn may_enter(&self, place: u64) -> bool {
        match self.waiting.front() {
            Some(front) if front.place == place => match front.want {
                Want::Shared => !self.is_publishing,
                Want::Exclusive => !self.is_publishing && self.readers == 0,
            },
            _ => false,
        }
    }

    /// Wake only the waiters whose turn it now is, a run of readers together
    fn wake_front(&self) {
        let Some(front) = self.waiting.front() else {
            return;
        };
        if !self.may_enter(front.place) {
            return;
        }
        match front.want {
            Want::Exclusive => front.thread.unpark(),
            Want::Shared => {
                for waiter in self
                    .waiting
                    .iter()
                    .take_while(|waiter| waiter.want == Want::Shared)
                {
                    waiter.thread.unpark();
                }
            }
        }
    }
}

/// A whole-set read tries this many fills outside the queue before it joins it
const OPTIMISTIC_FILLS: usize = 1;

/// Started and finished batch counts, which a whole-set read checks itself against
#[derive(Debug, Default)]
#[repr(align(128))]
struct Publishes {
    started: AtomicU64,
    finished: AtomicU64,
}

/// Batches moving the index under one hold of the barrier
#[derive(Debug, Default)]
struct Group {
    state: Mutex<(bool, usize)>,
    turn: Condvar,
}

/// A batch's place in its group, given up when its moves return or unwind
struct Moving<'group> {
    barrier: &'group PublishBarrier,
    group: &'group Group,
    leads: bool,
}

impl Drop for Moving<'_> {
    fn drop(&mut self) {
        if !self.leads {
            let mut state = lock(&self.group.state);
            state.1 -= 1;
            if state.1 == 0 {
                self.group.turn.notify_all();
            }
            return;
        }
        *lock(&self.barrier.gathering) = None;
        let mut state = lock(&self.group.state);
        while state.1 > 0 {
            state = self
                .group
                .turn
                .wait(state)
                .unwrap_or_else(PoisonError::into_inner);
        }
    }
}

/// A fair queue that keeps a batch's index moves apart from reads spanning several keys
#[derive(Debug)]
pub struct PublishBarrier {
    queue: Mutex<Queue>,
    publishes: Publishes,
    gathering: Mutex<Option<Arc<Group>>>,
}

impl PublishBarrier {
    /// A barrier nobody is holding
    pub fn new() -> PublishBarrier {
        PublishBarrier {
            queue: Mutex::default(),
            publishes: Publishes::default(),
            gathering: Mutex::default(),
        }
    }

    /// Hold the barrier for a read resolving several keys at once
    pub fn reading(&self) -> PublishGuard<'_> {
        self.enter(Want::Shared);
        PublishGuard {
            barrier: self,
            want: Want::Shared,
        }
    }

    /// Hold the barrier while something moves the index
    pub fn publishing(&self) -> PublishGuard<'_> {
        self.enter(Want::Exclusive);
        self.publishes.started.fetch_add(1, Ordering::AcqRel);
        PublishGuard {
            barrier: self,
            want: Want::Exclusive,
        }
    }

    /// Run a batch's moves under the barrier, beside every batch that arrives meanwhile
    pub fn publish_grouped<Moved>(&self, moves: impl FnOnce() -> Moved) -> Moved {
        let mut gathering = lock(&self.gathering);
        if let Some(group) = gathering.clone() {
            lock(&group.state).1 += 1;
            drop(gathering);
            let mut state = lock(&group.state);
            while !state.0 {
                state = group
                    .turn
                    .wait(state)
                    .unwrap_or_else(PoisonError::into_inner);
            }
            drop(state);
            let _moving = Moving {
                barrier: self,
                group: &group,
                leads: false,
            };
            return moves();
        }
        let group = Arc::new(Group::default());
        *gathering = Some(Arc::clone(&group));
        drop(gathering);

        let _publishing = self.publishing();
        lock(&group.state).0 = true;
        group.turn.notify_all();
        let _moving = Moving {
            barrier: self,
            group: &group,
            leads: true,
        };
        moves()
    }

    /// Serve a read over keys it cannot list, where `fill` may run more than once
    pub fn reading_all<Filled>(&self, mut fill: impl FnMut() -> Filled) -> Filled {
        for _ in 0..OPTIMISTIC_FILLS {
            let Some(quiet) = self.quiet() else {
                break;
            };
            let filled = fill();
            if self.publishes.started.load(Ordering::Acquire) == quiet {
                return filled;
            }
        }
        let _reading = self.reading();
        fill()
    }

    /// The started count while no batch is publishing, or nothing while one is
    fn quiet(&self) -> Option<u64> {
        let started = self.publishes.started.load(Ordering::Acquire);
        let finished = self.publishes.finished.load(Ordering::Acquire);
        (started == finished).then_some(started)
    }

    /// Join the queue and wait for it to be this caller's turn
    fn enter(&self, want: Want) {
        let mut queue = lock(&self.queue);
        let place = queue.next_place;
        queue.next_place += 1;
        queue.waiting.push_back(Waiter {
            place,
            want,
            thread: thread::current(),
        });
        // Queued before unlocking so no wake is lost, and the loop rechecks a stale wake
        while !queue.may_enter(place) {
            drop(queue);
            thread::park();
            queue = lock(&self.queue);
        }
        queue.waiting.pop_front();
        match want {
            Want::Shared => {
                queue.readers += 1;
                queue.wake_front();
            }
            Want::Exclusive => queue.is_publishing = true,
        }
    }

    /// Give the barrier up and hand the queue to whoever is next
    fn leave(&self, want: Want) {
        let mut queue = lock(&self.queue);
        match want {
            Want::Shared => queue.readers -= 1,
            Want::Exclusive => queue.is_publishing = false,
        }
        queue.wake_front();
    }
}

/// A held place in the barrier, given back when it drops
#[derive(Debug)]
pub struct PublishGuard<'barrier> {
    barrier: &'barrier PublishBarrier,
    want: Want,
}

impl Drop for PublishGuard<'_> {
    fn drop(&mut self) {
        // Count the batch finished before the barrier goes back
        if self.want == Want::Exclusive {
            self.barrier
                .publishes
                .finished
                .fetch_add(1, Ordering::AcqRel);
        }
        self.barrier.leave(self.want);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    // a reader waiting behind publishers is served within one round of them
    #[test]
    fn a_reader_is_not_starved() {
        let barrier = Arc::new(PublishBarrier::new());
        let is_running = Arc::new(AtomicBool::new(true));
        let publishes = Arc::new(AtomicUsize::new(0));

        let mut writers = Vec::new();
        for _ in 0..8 {
            let barrier = Arc::clone(&barrier);
            let is_running = Arc::clone(&is_running);
            let publishes = Arc::clone(&publishes);
            writers.push(std::thread::spawn(move || {
                while is_running.load(Ordering::Relaxed) {
                    let _held = barrier.publishing();
                    publishes.fetch_add(1, Ordering::Relaxed);
                }
            }));
        }
        // Let the writers get going, so the read below arrives into contention.
        while publishes.load(Ordering::Relaxed) < 1_000 {
            std::hint::spin_loop();
        }

        let began = Instant::now();
        for _ in 0..100 {
            drop(barrier.reading());
        }
        let took = began.elapsed();

        is_running.store(false, Ordering::Relaxed);
        for writer in writers {
            writer.join().expect("writer");
        }

        assert!(
            took < Duration::from_millis(500),
            "100 reads took {took:?} against eight publishers, which is starvation"
        );
    }

    // several readers hold it at once
    #[test]
    fn readers_share() {
        let barrier = Arc::new(PublishBarrier::new());
        let inside = Arc::new(AtomicUsize::new(0));
        let both_in = Arc::new(AtomicBool::new(false));

        let mut readers = Vec::new();
        for _ in 0..2 {
            let barrier = Arc::clone(&barrier);
            let inside = Arc::clone(&inside);
            let both_in = Arc::clone(&both_in);
            readers.push(std::thread::spawn(move || {
                let _held = barrier.reading();
                inside.fetch_add(1, Ordering::SeqCst);
                let deadline = Instant::now() + Duration::from_secs(5);
                while Instant::now() < deadline {
                    if inside.load(Ordering::SeqCst) == 2 {
                        both_in.store(true, Ordering::SeqCst);
                        break;
                    }
                    std::hint::spin_loop();
                }
            }));
        }
        for reader in readers {
            reader.join().expect("reader");
        }

        assert!(both_in.load(Ordering::SeqCst), "two reads never overlapped");
    }

    // a publisher and a reader never hold it at the same time
    #[test]
    fn publishing_excludes() {
        let barrier = Arc::new(PublishBarrier::new());
        let readers_inside = Arc::new(AtomicUsize::new(0));
        let saw_overlap = Arc::new(AtomicBool::new(false));
        let is_running = Arc::new(AtomicBool::new(true));

        let publisher = {
            let barrier = Arc::clone(&barrier);
            let readers_inside = Arc::clone(&readers_inside);
            let saw_overlap = Arc::clone(&saw_overlap);
            let is_running = Arc::clone(&is_running);
            std::thread::spawn(move || {
                while is_running.load(Ordering::Relaxed) {
                    let _held = barrier.publishing();
                    if readers_inside.load(Ordering::SeqCst) != 0 {
                        saw_overlap.store(true, Ordering::SeqCst);
                    }
                }
            })
        };

        for _ in 0..2_000 {
            let _held = barrier.reading();
            readers_inside.fetch_add(1, Ordering::SeqCst);
            readers_inside.fetch_sub(1, Ordering::SeqCst);
        }
        is_running.store(false, Ordering::Relaxed);
        publisher.join().expect("publisher");

        assert!(
            !saw_overlap.load(Ordering::SeqCst),
            "a publish saw a reader inside"
        );
    }

    // a joiner whose moves panic still lets its leader give the barrier back
    #[test]
    fn a_panicking_joiner_frees_the_leader() {
        let barrier = Arc::new(PublishBarrier::new());
        let joined = Arc::new(AtomicBool::new(false));
        let (led, done) = std::sync::mpsc::channel();

        let leader = {
            let barrier = Arc::clone(&barrier);
            let joined = Arc::clone(&joined);
            std::thread::spawn(move || {
                barrier.publish_grouped(|| {
                    while !joined.load(Ordering::SeqCst) {
                        std::thread::yield_now();
                    }
                });
                led.send(()).expect("send");
            })
        };
        while lock(&barrier.gathering).is_none() {
            std::thread::yield_now();
        }
        let joiner = {
            let barrier = Arc::clone(&barrier);
            let joined = Arc::clone(&joined);
            std::thread::spawn(move || {
                barrier.publish_grouped(|| {
                    joined.store(true, Ordering::SeqCst);
                    panic!("a joiner's moves failed");
                })
            })
        };

        assert!(joiner.join().is_err(), "the joiner did not panic");
        done.recv_timeout(Duration::from_secs(10))
            .expect("the leader never gave the barrier back");
        leader.join().expect("leader");
        assert!(lock(&barrier.gathering).is_none());
        drop(barrier.publishing());
    }
}
