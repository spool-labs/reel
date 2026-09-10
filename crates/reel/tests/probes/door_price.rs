//! What asyncifying work that never waits would cost, at the structure alone
//!
//! An index probe is hundreds of nanoseconds of RAM work with nothing to overlap, so
//! whether that flow should be a future is arithmetic on what the future machinery
//! itself costs. Three prices: the bare probe, the same probe behind a state machine a
//! perfect executor polls once, and one genuine suspension. The awaited arms are priced
//! generously, with one waker a thread built outside the loop, futures pinned on the
//! stack and nothing allocated per op, so the gaps are floors.
//!
//! Opt-in. Run with:
//!   cargo test -p tape-reel --release --test probes -- door_price

use std::future::Future;
use std::pin::{pin, Pin};
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use std::thread::{self, Thread};
use std::time::Instant;

use reel::format::loc::{Loc, SegmentId};
use reel::format::lsn::Lsn;
use reel::{Entry, OpenTable};

/// Keys the bare table holds
const TABLE_KEYS: usize = 1_000_000;

/// Asks each thread makes per timed arm
const ASKS: u64 = 1_000_000;

/// The one mixing function every draw here goes through
fn mixed(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

/// A scattered thirty-two byte key
fn key_of(at: u64) -> [u8; 32] {
    let mut key = [0u8; 32];
    for word in 0..4u64 {
        key[word as usize * 8..][..8].copy_from_slice(&mixed(at * 4 + word).to_le_bytes());
    }
    key
}

/// A wake unparks the thread that polls
struct Unparker(Thread);

impl Wake for Unparker {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}

/// Drive one future on the calling thread with a waker built once by the caller
fn drive<Answered: Future>(waker: &Waker, future: Answered) -> Answered::Output {
    let mut future = pin!(future);
    let mut context = Context::from_waker(waker);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(answer) => return answer,
            Poll::Pending => thread::park(),
        }
    }
}

/// Pending once with its own wake already rung, ready the second time
///
/// The cheapest genuine suspension there is: the repoll is immediate and on the same
/// thread, so what it prices is the machinery of suspending and nothing else.
struct YieldOnce(bool);

impl Future for YieldOnce {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<()> {
        match self.0 {
            true => Poll::Ready(()),
            false => {
                self.0 = true;
                context.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }
}

/// Per-op nanoseconds for one arm at one thread count, hits asserted whole
fn timed<Ask>(threads: u64, ask: Ask) -> f64
where
    Ask: Fn(u64) -> u64 + Send + Sync + Copy,
{
    let began = Instant::now();
    let mut found = 0u64;
    thread::scope(|scope| {
        let asked: Vec<_> = (0..threads)
            .map(|thread| scope.spawn(move || ask(thread)))
            .collect();
        for thread in asked {
            found += thread.join().expect("a probe thread");
        }
    });
    began.elapsed().as_nanos() as f64 / found as f64
}

/// The bare structure: a probe against a state machine around the same probe
pub fn a_probe_never_waits() {
    let mut table: OpenTable<32, Entry> = OpenTable::with_keys(TABLE_KEYS);
    for at in 0..TABLE_KEYS as u64 {
        table.insert(
            key_of(at),
            Entry::new(Loc::new(SegmentId(1), at as u32, 200), Lsn(at)),
        );
    }
    let table = &table;

    println!("one probe of {TABLE_KEYS} resident keys, ns an ask, {ASKS} asks a thread");
    println!("  threads      direct   future-ready   one-suspension");
    for threads in [1u64, 8] {
        let direct = timed(threads, |thread| {
            let mut found = 0u64;
            for at in 0..ASKS {
                let key = key_of(mixed(thread * ASKS + at) % TABLE_KEYS as u64);
                found += u64::from(table.get(std::hint::black_box(&key)).is_some());
            }
            assert_eq!(found, ASKS, "every drawn key is present");
            found
        });
        let ready = timed(threads, |thread| {
            let waker = Waker::from(Arc::new(Unparker(thread::current())));
            let mut found = 0u64;
            for at in 0..ASKS {
                let key = key_of(mixed(thread * ASKS + at) % TABLE_KEYS as u64);
                found += u64::from(drive(&waker, async {
                    table.get(std::hint::black_box(&key)).is_some()
                }));
            }
            assert_eq!(found, ASKS, "the state machine loses nothing");
            found
        });
        let suspended = timed(threads, |thread| {
            let waker = Waker::from(Arc::new(Unparker(thread::current())));
            let mut found = 0u64;
            for at in 0..ASKS {
                let key = key_of(mixed(thread * ASKS + at) % TABLE_KEYS as u64);
                found += u64::from(drive(&waker, async {
                    YieldOnce(false).await;
                    table.get(std::hint::black_box(&key)).is_some()
                }));
            }
            assert_eq!(found, ASKS, "a suspension loses nothing");
            found
        });
        println!("  {threads:>7}   {direct:>8.1}   {ready:>12.1}   {suspended:>14.1}");
    }
}
