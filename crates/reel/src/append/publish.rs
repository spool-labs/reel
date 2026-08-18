//! The barrier that makes a batch's index moves land together
//!
//! A batch reaches the device as one reservation, one write and one sync, and only then
//! moves the index key by key. A batch holds this exclusively while it moves the index
//! and a read of several keys holds it shared, so such a read sees all or none.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::thread::{self, Thread};

use crate::format::column::{ColumnId, RecordKey};
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
    ///
    /// A reader behind a waiting publisher stays behind it even though the lock is
    /// readable, which is what keeps publishers from stepping in front of it forever.
    fn may_enter(&self, place: u64) -> bool {
        match self.waiting.front() {
            Some(front) if front.place == place => match front.want {
                Want::Shared => !self.is_publishing,
                Want::Exclusive => !self.is_publishing && self.readers == 0,
            },
            _ => false,
        }
    }

    /// Wake the waiters whose turn it now is, by name, and no others
    ///
    /// Waking the front by name is one wake per handover however many are waiting. A run
    /// of readers at the front is woken together, since they do not exclude each other.
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

/// Stripes the barrier is split into
///
/// One queue for the whole volume makes every batch wait for every other batch, so a
/// batch takes only the stripes its own keys fall in. Sixty-four is what a u64 holds,
/// which is what lets a batch name its whole set in one word.
pub const PUBLISH_STRIPES: u32 = 64;

/// Every stripe, for an operation whose answer spans keys it cannot name
///
/// Derived rather than written down: a mask with bits above the stripe count set would
/// send an entering caller off the end of the array.
pub const ALL_STRIPES: u64 = match PUBLISH_STRIPES {
    64 => u64::MAX,
    count => (1u64 << count) - 1,
};

/// Leading key bytes the stripe is chosen by
///
/// The same bytes a column shards on, so the common batch takes a single stripe.
const STRIPE_KEY_BYTES: usize = 2;

/// The stripe a key belongs to
///
/// What atomicity needs is that a reader and a writer touching the same key take the
/// same stripe: taking more stripes than needed is safe, taking the wrong one is not.
pub fn stripe_of(key: &RecordKey) -> u32 {
    stripe_of_parts(key.column, key.as_slice())
}

/// The same stripe from a key held as its parts, for a caller holding a move
pub fn stripe_of_parts(column: ColumnId, key: &[u8]) -> u32 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    hash ^= column.as_index() as u64;
    hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    for byte in key.iter().take(STRIPE_KEY_BYTES) {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    (hash % u64::from(PUBLISH_STRIPES)) as u32
}

/// The set of stripes a run of keys falls in, as a mask
pub fn stripes_of<'keys>(keys: impl IntoIterator<Item = &'keys RecordKey>) -> u64 {
    keys.into_iter()
        .fold(0, |mask, key| mask | 1u64 << stripe_of(key))
}

/// One stripe's queue, which is the whole barrier for the keys that fall in it
///
/// Aligned to a cache line and padded to fill it, or the split trades one contended
/// lock for a line several cores write to at once.
#[derive(Debug, Default)]
#[repr(align(128))]
struct Stripe {
    queue: Mutex<Queue>,
}

/// How many times a whole-set read fills before it stops trying to dodge the queue
///
/// A fill a batch landed under is thrown away, and a batch landing means the fair queue
/// is where a reader belongs.
const OPTIMISTIC_FILLS: usize = 1;

/// The batch counts a whole-set read checks itself against
///
/// Two counts rather than one word carrying a parity bit, because stripes let batches
/// publish at once and a second publisher would flip a parity back to looking quiet.
/// Started apart from finished says both that one is running and that one has run.
#[derive(Debug, Default)]
#[repr(align(128))]
struct Publishes {
    started: AtomicU64,
    finished: AtomicU64,
}

/// Holds a batch's index moves apart from the reads that span several keys
///
/// A fair queue rather than a plain reader-writer lock: arrivals are served in the order
/// they arrive, so a reader waits for the publishers already queued and no more. It is
/// no snapshot, since the index holds one version per key and nothing older to offer.
#[derive(Debug)]
pub struct PublishBarrier {
    stripes: Box<[Stripe]>,
    publishes: Publishes,
}

impl PublishBarrier {
    /// A barrier nobody is holding
    pub fn new() -> PublishBarrier {
        PublishBarrier {
            stripes: (0..PUBLISH_STRIPES).map(|_| Stripe::default()).collect(),
            publishes: Publishes::default(),
        }
    }

    /// Hold the barrier for a read resolving several keys at once
    ///
    /// Held across the index lookups and dropped before the records are read, so a batch
    /// waiting to publish waits on memory rather than on the volume.
    pub fn reading(&self, stripes: u64) -> PublishGuard<'_> {
        self.enter(stripes, Want::Shared);
        PublishGuard {
            barrier: self,
            want: Want::Shared,
            stripes,
        }
    }

    /// Hold the barrier while a batch moves the index
    ///
    /// The count rises once the stripes are held and falls as they are given back, so it
    /// brackets the index moves rather than the wait for them.
    pub fn publishing(&self, stripes: u64) -> PublishGuard<'_> {
        self.enter(stripes, Want::Exclusive);
        self.publishes.started.fetch_add(1, Ordering::AcqRel);
        PublishGuard {
            barrier: self,
            want: Want::Exclusive,
            stripes,
        }
    }

    /// Serve a read spanning keys it cannot name, without taking every stripe
    ///
    /// The fill runs holding no stripe and its answer is kept only if no batch published
    /// across it. It may be called more than once, so a caller carrying state into it
    /// puts that state back itself, and a fill with an effect belongs under the stripes.
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
        let _reading = self.reading(ALL_STRIPES);
        fill()
    }

    /// The started count while no batch is publishing, or nothing while one is
    ///
    /// A batch that finishes between the two loads reads as quiet and is not a false
    /// one: it finished before the fill began, and the acquire carries its moves in.
    fn quiet(&self) -> Option<u64> {
        let started = self.publishes.started.load(Ordering::Acquire);
        let finished = self.publishes.finished.load(Ordering::Acquire);
        (started == finished).then_some(started)
    }

    /// Take every stripe in the set, lowest first
    ///
    /// The order is the whole of the deadlock argument: a caller only ever waits on a
    /// stripe above the ones it already holds.
    fn enter(&self, stripes: u64, want: Want) {
        let mut rest = stripes;
        while rest != 0 {
            let at = rest.trailing_zeros();
            rest &= rest - 1;
            self.enter_one(at as usize, want);
        }
    }

    /// Give every stripe in the set back
    ///
    /// Order does not matter on the way out, since nothing is acquired here.
    fn leave(&self, stripes: u64, want: Want) {
        let mut rest = stripes;
        while rest != 0 {
            let at = rest.trailing_zeros();
            rest &= rest - 1;
            self.leave_one(at as usize, want);
        }
    }

    /// Join one stripe's queue and wait for it to be this caller's turn
    fn enter_one(&self, at: usize, want: Want) {
        let mut queue = lock(&self.stripes[at].queue);
        let place = queue.next_place;
        queue.next_place += 1;
        queue.waiting.push_back(Waiter {
            place,
            want,
            thread: thread::current(),
        });
        // The waiter is in the queue before the lock is given up, so a handover landing
        // between the check and the park still has a thread to name. The loop rechecks
        // rather than trusts the token, which can be left over from an earlier wake.
        while !queue.may_enter(place) {
            drop(queue);
            thread::park();
            queue = lock(&self.stripes[at].queue);
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

    /// Give one stripe up and hand its queue to whoever is next
    fn leave_one(&self, at: usize, want: Want) {
        let mut queue = lock(&self.stripes[at].queue);
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
    stripes: u64,
}

impl Drop for PublishGuard<'_> {
    fn drop(&mut self) {
        // Before the stripes go back, so the count falls once the moves are done rather
        // than once the locks are free.
        if self.want == Want::Exclusive {
            self.barrier
                .publishes
                .finished
                .fetch_add(1, Ordering::AcqRel);
        }
        self.barrier.leave(self.stripes, self.want);
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
                    let _held = barrier.publishing(ALL_STRIPES);
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
            drop(barrier.reading(ALL_STRIPES));
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

    // several readers hold it at once rather than queueing behind each other
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
                let _held = barrier.reading(ALL_STRIPES);
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
                    let _held = barrier.publishing(ALL_STRIPES);
                    if readers_inside.load(Ordering::SeqCst) != 0 {
                        saw_overlap.store(true, Ordering::SeqCst);
                    }
                }
            })
        };

        for _ in 0..2_000 {
            let _held = barrier.reading(ALL_STRIPES);
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
}
