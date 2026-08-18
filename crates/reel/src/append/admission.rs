//! Per-volume in-flight byte budget that bounds admission
//!
//! A counting byte-semaphore: a record acquires its bytes and a writer that cannot
//! acquire blocks, so a stalled device becomes slow acknowledgements rather than an
//! unbounded queue. Per volume, never per tail and never process global, because the
//! drain rate that returns permits is a device property.

use crate::units::ByteCount;

use crate::sync::checked::{AtomicU64, Ordering};
use crate::sync::tension::{Tension, Wait};

/// A ceiling of zero disables the bound and admits every request immediately
const UNBOUNDED: u64 = 0;

/// Bytes one volume admits before a writer waits
///
/// Wide enough that an ordinary drain never meets it, narrow enough that a stalled
/// device becomes slow acknowledgements rather than an unbounded queue.
pub const DEFAULT_INFLIGHT_BYTES: u64 = 512 * 1024 * 1024;

/// Counting byte-semaphore bounding the bytes in flight against one volume
pub struct InflightBudget {
    /// The configured byte ceiling, zero for unbounded
    ceiling: u64,

    /// The ceiling in force now, which the maintenance tick lowers under pressure
    throttled: AtomicU64,

    /// Bytes acquired and not yet released, the queued-bytes gauge
    in_flight: AtomicU64,

    /// Every byte ever admitted, monotonic, for rate signals over a window
    admitted: AtomicU64,

    /// The waitlist carrying the writers that had to wait for room
    tension: Tension<()>,
}

impl Default for InflightBudget {
    fn default() -> InflightBudget {
        InflightBudget::new(ByteCount::from_bytes(DEFAULT_INFLIGHT_BYTES))
    }
}

impl InflightBudget {
    /// A budget with a byte ceiling, where zero leaves admission unbounded
    pub fn new(ceiling: ByteCount) -> InflightBudget {
        InflightBudget {
            ceiling: ceiling.to_bytes(),
            throttled: AtomicU64::new(ceiling.to_bytes()),
            in_flight: AtomicU64::new(0),
            admitted: AtomicU64::new(0),
            tension: Tension::new(()),
        }
    }

    /// Squeeze the budget to a share of its ceiling, for a volume filling up
    ///
    /// The share is of the configured ceiling rather than of whatever is in force, which
    /// is what stops repeated ticks ratcheting the budget toward nothing. A raise wakes
    /// the waiters itself, since a volume that stopped writing has no release to give.
    pub fn throttle(&self, share: f64) {
        if self.ceiling == UNBOUNDED {
            return;
        }
        let ceiling = (self.ceiling as f64 * share.clamp(0.0, 1.0)) as u64;
        if self.throttled.swap(ceiling, Ordering::SeqCst) < ceiling {
            self.tension.slack();
        }
    }

    /// The ceiling in force, which is the configured one until pressure lowers it
    pub fn effective_ceiling(&self) -> ByteCount {
        ByteCount::from_bytes(self.throttled.load(Ordering::Relaxed))
    }

    /// Whether this budget bounds admission at all
    pub fn is_bounded(&self) -> bool {
        self.ceiling != UNBOUNDED
    }

    /// Acquire bytes for a record, blocking until the budget has room
    ///
    /// A request larger than the whole ceiling waits until the volume is idle and then
    /// proceeds alone, so an oversized record can never deadlock the budget.
    pub fn acquire(&self, bytes: u64) {
        if self.try_acquire(bytes) {
            return;
        }
        self.tension.park(|_| self.try_acquire(bytes).then_some(()));
    }

    /// Acquire bytes as a future, for a writer with a worker worth keeping
    ///
    /// The same admission the blocking call takes, resolved when the budget has room. It
    /// is dearer where nothing waits, since every poll takes the gate.
    pub fn reserve(&self, bytes: u64) -> Wait<'_, (), impl FnMut(&mut ()) -> Option<()> + '_> {
        self.tension
            .wait(move |_: &mut ()| self.try_acquire(bytes).then_some(()))
    }

    /// Release bytes a completed record held, waking any waiting writer
    ///
    /// The subtraction is unconditional rather than a compare and swap clamping at
    /// zero, since every release answers an acquire of the same count.
    pub fn release(&self, bytes: u64) {
        let held = self.in_flight.fetch_sub(bytes, Ordering::SeqCst);
        debug_assert!(held >= bytes, "a release answered no acquire");
        self.tension.slack();
    }

    /// Outstanding acquired bytes, the queued-bytes gauge
    pub fn queued_bytes(&self) -> ByteCount {
        ByteCount::from_bytes(self.in_flight.load(Ordering::Relaxed))
    }

    /// Every byte this budget has ever admitted, whatever its bound
    pub fn admitted_total(&self) -> u64 {
        self.admitted.load(Ordering::Relaxed)
    }

    fn try_acquire(&self, bytes: u64) -> bool {
        if self.ceiling == UNBOUNDED {
            self.in_flight.fetch_add(bytes, Ordering::SeqCst);
            self.admitted.fetch_add(bytes, Ordering::Relaxed);
            return true;
        }
        let mut held = self.in_flight.load(Ordering::SeqCst);
        loop {
            if !self.can_admit(held, bytes) {
                return false;
            }
            match self.in_flight.compare_exchange_weak(
                held,
                held + bytes,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => {
                    self.admitted.fetch_add(bytes, Ordering::Relaxed);
                    return true;
                }
                Err(seen) => held = seen,
            }
        }
    }

    /// The idle escape is what keeps a throttle a slowdown rather than a stall: however
    /// far the budget has been squeezed, a writer that finds it empty proceeds.
    fn can_admit(&self, in_flight: u64, bytes: u64) -> bool {
        if in_flight == 0 {
            return true;
        }
        in_flight + bytes <= self.throttled.load(Ordering::SeqCst)
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;

    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    use crate::sync::tension::block_on;

    fn budget(bytes: u64) -> InflightBudget {
        InflightBudget::new(ByteCount::from_bytes(bytes))
    }

    // an unbounded budget admits without blocking and still tracks the gauge
    #[test]
    fn unbounded_admits() {
        let budget = budget(UNBOUNDED);

        budget.acquire(4096);
        budget.acquire(8192);

        assert!(!budget.is_bounded());
        assert_eq!(budget.queued_bytes(), ByteCount::from_bytes(12_288));
    }

    // acquire and release move the gauge up and back down
    #[test]
    fn gauge_tracks_outstanding() {
        let budget = budget(1024);

        budget.acquire(1024);
        assert_eq!(budget.queued_bytes(), ByteCount::from_bytes(1024));

        budget.release(1024);
        assert_eq!(budget.queued_bytes(), ByteCount::from_bytes(0));
    }

    // a request larger than the whole ceiling still proceeds when idle
    #[test]
    fn oversized_proceeds_idle() {
        let budget = budget(1024);

        budget.acquire(4096);

        assert_eq!(budget.queued_bytes(), ByteCount::from_bytes(4096));
    }

    // an exhausted budget blocks a further acquire until a release frees room
    #[test]
    fn exhausted_blocks_then_proceeds() {
        let budget = Arc::new(budget(1024));
        budget.acquire(1024);

        let waiter_budget = Arc::clone(&budget);
        let has_acquired = Arc::new(AtomicBool::new(false));
        let waiter_flag = Arc::clone(&has_acquired);
        let waiter = thread::spawn(move || {
            waiter_budget.acquire(1024);
            waiter_flag.store(true, Ordering::SeqCst);
        });

        thread::sleep(Duration::from_millis(50));
        assert!(!has_acquired.load(Ordering::SeqCst));
        assert_eq!(budget.queued_bytes(), ByteCount::from_bytes(1024));

        budget.release(1024);
        waiter.join().expect("waiter joins");
        assert!(has_acquired.load(Ordering::SeqCst));
    }

    // a budget with room answers a reservation without ever leaving a waiter
    #[test]
    fn reserve_admits_without_waiting() {
        let budget = budget(4096);

        block_on(budget.reserve(1024));

        assert_eq!(budget.queued_bytes(), ByteCount::from_bytes(1024));
    }

    // an exhausted budget leaves the reservation pending until a release frees room
    #[test]
    fn reserve_waits_for_release() {
        let budget = Arc::new(budget(1024));
        budget.acquire(1024);

        let waiter_budget = Arc::clone(&budget);
        let has_acquired = Arc::new(AtomicBool::new(false));
        let waiter_flag = Arc::clone(&has_acquired);
        let waiter = thread::spawn(move || {
            block_on(waiter_budget.reserve(1024));
            waiter_flag.store(true, Ordering::SeqCst);
        });

        thread::sleep(Duration::from_millis(50));
        assert!(!has_acquired.load(Ordering::SeqCst));

        budget.release(1024);
        waiter.join().expect("waiter joins");
        assert!(has_acquired.load(Ordering::SeqCst));
        assert_eq!(budget.queued_bytes(), ByteCount::from_bytes(1024));
    }

    // a reservation dropped before it is admitted leaves nothing behind
    #[test]
    fn dropped_reserve_leaves_the_budget_alone() {
        let budget = budget(1024);
        budget.acquire(1024);

        {
            let mut pending = Box::pin(budget.reserve(512));
            let waker = std::task::Waker::noop();
            let mut cx = std::task::Context::from_waker(waker);
            assert!(std::future::Future::poll(pending.as_mut(), &mut cx).is_pending());
        }

        budget.release(1024);
        assert_eq!(budget.queued_bytes(), ByteCount::from_bytes(0));
    }

    // a parked writer and a pending reservation both clear on one release
    #[test]
    fn park_and_reserve_clear_together() {
        let budget = Arc::new(budget(2048));
        budget.acquire(2048);

        let parked_budget = Arc::clone(&budget);
        let parked = thread::spawn(move || {
            parked_budget.acquire(1024);
            parked_budget.release(1024);
        });
        let seated_budget = Arc::clone(&budget);
        let seated = thread::spawn(move || {
            block_on(seated_budget.reserve(1024));
            seated_budget.release(1024);
        });

        thread::sleep(Duration::from_millis(50));
        assert_eq!(budget.queued_bytes(), ByteCount::from_bytes(2048));

        budget.release(2048);
        parked.join().expect("parked writer joins");
        seated.join().expect("seated writer joins");
        assert_eq!(budget.queued_bytes(), ByteCount::from_bytes(0));
    }

    // a throttle takes its share of the configured ceiling, not of the last one
    #[test]
    fn throttle_is_always_a_share_of_the_configured_ceiling() {
        let budget = budget(1000);

        budget.throttle(0.5);
        assert_eq!(budget.effective_ceiling(), ByteCount::from_bytes(500));
        // Were this relative to what is in force, a second half would leave 250.
        budget.throttle(0.5);
        assert_eq!(budget.effective_ceiling(), ByteCount::from_bytes(500));

        budget.throttle(1.0);
        assert_eq!(budget.effective_ceiling(), ByteCount::from_bytes(1000));
    }

    // a squeezed budget makes a writer wait that the full one would have admitted
    #[test]
    fn throttle_holds_back_a_writer_the_ceiling_would_admit() {
        let budget = Arc::new(budget(1024));
        budget.throttle(0.25);
        budget.acquire(256);

        let waiter_budget = Arc::clone(&budget);
        let has_acquired = Arc::new(AtomicBool::new(false));
        let waiter_flag = Arc::clone(&has_acquired);
        let waiter = thread::spawn(move || {
            waiter_budget.acquire(256);
            waiter_flag.store(true, Ordering::SeqCst);
        });

        thread::sleep(Duration::from_millis(50));
        assert!(
            !has_acquired.load(Ordering::SeqCst),
            "512 fits 1024 but not the squeeze"
        );

        budget.release(256);
        waiter.join().expect("waiter joins");
        assert!(has_acquired.load(Ordering::SeqCst));
    }

    // raising the ceiling wakes the writers, who have no release coming
    #[test]
    fn a_raise_wakes_what_the_squeeze_parked() {
        let budget = Arc::new(budget(1024));
        budget.throttle(0.25);
        budget.acquire(256);

        let parked_budget = Arc::clone(&budget);
        let parked = thread::spawn(move || parked_budget.acquire(256));
        let seated_budget = Arc::clone(&budget);
        let seated = thread::spawn(move || block_on(seated_budget.reserve(256)));

        thread::sleep(Duration::from_millis(50));
        assert!(budget.tension.is_taut(), "both doors should be waiting");

        // No release is coming, so the raise itself has to be the wakeup.
        budget.throttle(1.0);
        parked.join().expect("parked writer joins");
        seated.join().expect("seated writer joins");
    }

    // however hard the squeeze, an idle budget still admits, so a band cannot stall
    #[test]
    fn a_throttled_budget_still_admits_when_idle() {
        let budget = budget(1024);
        budget.throttle(0.0);

        budget.acquire(4096);

        assert_eq!(budget.queued_bytes(), ByteCount::from_bytes(4096));
    }

    // an unbounded budget has no ceiling to take a share of
    #[test]
    fn throttle_leaves_an_unbounded_budget_alone() {
        let budget = budget(UNBOUNDED);

        budget.throttle(0.0);
        budget.acquire(4096);

        assert!(!budget.is_bounded());
        assert_eq!(budget.queued_bytes(), ByteCount::from_bytes(4096));
    }

    // a stalled device caps the gauge instead of growing memory without bound
    #[test]
    fn stalled_device_caps_gauge() {
        let budget = Arc::new(budget(2048));
        budget.acquire(2048);

        let mut waiters = Vec::new();
        for _ in 0..8 {
            let blocked = Arc::clone(&budget);
            waiters.push(thread::spawn(move || {
                blocked.acquire(512);
                blocked.release(512);
            }));
        }

        thread::sleep(Duration::from_millis(50));
        assert_eq!(budget.queued_bytes(), ByteCount::from_bytes(2048));

        budget.release(2048);
        for waiter in waiters {
            waiter.join().expect("waiter joins");
        }
        assert_eq!(budget.queued_bytes(), ByteCount::from_bytes(0));
    }
}

// Not model-checked: loom treats SeqCst as AcqRel, so it permits the store-then-load
// reordering across the release decrement and the waiting count, which is the one
// reordering SeqCst forbids and the only way to lose a wakeup here.
