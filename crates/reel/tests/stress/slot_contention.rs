//! What the completion slots cost when callers stack up on them
//!
//! Every claim, waker seat, deposit and take on the async door goes through the
//! slot table, so this drives the table alone with no device under it, in the three
//! shapes the door meets: inline files the completion before the first poll, pending
//! seats the waker first and lands on the caller's own thread, detached lands in
//! batches on a thread of its own. The completions carry no payload, so a row
//! measures the table's bookkeeping and the locks around it.
//!
//! Ignored by default, since it spawns threads and holds cores. Run with:
//!   cargo test -p reel --test slot_contention --release -- --ignored --nocapture --test-threads=1
//!
//! Knobs, all optional: REEL_SLOT_MODES, REEL_SLOT_THREADS, REEL_SLOT_DEPTHS,
//! REEL_SLOT_OPS.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::task::{Context, Wake, Waker};
use std::thread::{self, Thread};
use std::time::Instant;

use reel::io::op::{Completion, Outcome, Tag};
use reel::io::slots::{IoWait, SlotTable};

/// Caller threads the matrix sweeps, which is the axis the ceiling shows up on
const DEFAULT_THREADS: &str = "1,2,4,8";

/// Ops in flight per caller thread
const DEFAULT_DEPTHS: &str = "8,32";

/// Ops each caller thread drives per cell
const DEFAULT_OPS: u64 = 300_000;

/// How a cell's completions reach the futures waiting for them
#[derive(Clone, Copy, Eq, PartialEq)]
enum Mode {
    /// Filed before the first poll, so the future takes it and never pends
    Inline,

    /// Seated first and filed by the caller itself, so the waker path runs
    Pending,

    /// Filed in batches by a thread of its own, which is the ring engine's shape
    Detached,
}

impl Mode {
    fn name(self) -> &'static str {
        match self {
            Mode::Inline => "inline",
            Mode::Pending => "pending",
            Mode::Detached => "detached",
        }
    }
}

fn env_string(name: &str, fallback: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| fallback.to_string())
}

fn env_list(name: &str, fallback: &str) -> Vec<u64> {
    env_string(name, fallback)
        .split(',')
        .filter_map(|item| item.trim().parse().ok())
        .collect()
}

fn env_num(name: &str, fallback: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.trim().parse().ok())
        .unwrap_or(fallback)
}

fn modes() -> Vec<Mode> {
    env_string("REEL_SLOT_MODES", "inline,pending,detached")
        .split(',')
        .filter_map(|name| match name.trim() {
            "inline" => Some(Mode::Inline),
            "pending" => Some(Mode::Pending),
            "detached" => Some(Mode::Detached),
            _ => None,
        })
        .collect()
}

/// A waker that unparks the thread polling the set, the smallest real executor
struct Unparker(Thread);

impl Wake for Unparker {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}

/// What one caller hands the lander, one queue per caller so the handoff is not
/// what the cell measures
struct Inbox {
    /// Tags handed over, per caller thread
    handed: Vec<Mutex<Vec<Tag>>>,

    /// Caller threads still driving ops
    running: AtomicU64,
}

impl Inbox {
    fn new(callers: usize) -> Inbox {
        let mut handed = Vec::with_capacity(callers);
        for _ in 0..callers {
            handed.push(Mutex::new(Vec::new()));
        }
        Inbox {
            handed,
            running: AtomicU64::new(callers as u64),
        }
    }

    fn hand(&self, caller: usize, tag: Tag) {
        let mut queue = self.handed[caller]
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        queue.push(tag);
    }

    fn take(&self, caller: usize, into: &mut Vec<Tag>) {
        let mut queue = self.handed[caller]
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        into.append(&mut queue);
    }
}

fn answered(tag: Tag) -> Completion {
    Completion {
        tag,
        outcome: Outcome::Done(Ok(())),
    }
}

/// File what the callers hand over, until the last of them has stopped handing
///
/// A caller only stops once every tag it handed has come back, so a sweep that
/// finds nothing with no caller left is a lander with nothing owed.
fn land(table: &SlotTable, inbox: &Inbox) {
    let mut tags: Vec<Tag> = Vec::new();
    let mut batch: Vec<Completion> = Vec::new();
    loop {
        let mut moved = 0;
        for caller in 0..inbox.handed.len() {
            inbox.take(caller, &mut tags);
            moved += tags.len();
            for tag in tags.drain(..) {
                batch.push(answered(tag));
            }
            if !batch.is_empty() {
                table.file(&mut batch);
            }
        }
        if moved > 0 {
            continue;
        }
        if inbox.running.load(Ordering::Acquire) == 0 {
            return;
        }
        // Holding the core to find out again that the callers are behind costs them
        // the core they are behind on.
        thread::yield_now();
    }
}

/// One op's place in a caller's set, which is the two steps the door takes
struct Flight<'table> {
    /// The tag drawn for the op, which is the slot it wants
    tag: Tag,

    /// The wait on its completion, absent until the slot is claimed
    wait: Option<IoWait<'table>>,

    /// Whether the completion has been handed over yet
    is_handed: bool,
}

/// How far one flight got in a sweep
enum Step {
    /// Nothing this flight could do until something else changes
    Stalled,

    /// The flight took its slot, was handed over, or landed its completion
    Moved,

    /// The completion is in, so the flight is over
    Answered,
}

/// Take one flight as far as it will go right now
///
/// The claim is polled as a fresh future every sweep rather than held: seating is
/// idempotent, and a caller blocked on one claim would hold the slots its other
/// flights already took.
fn advance<'table>(
    table: &'table SlotTable,
    mode: Mode,
    caller: usize,
    inbox: &Inbox,
    flight: &mut Flight<'table>,
    cx: &mut Context<'_>,
) -> Step {
    if flight.wait.is_none() {
        let wanted = [flight.tag];
        let mut claiming = table.claim(&wanted);
        if Pin::new(&mut claiming).poll(cx).is_pending() {
            return Step::Stalled;
        }
        flight.wait = Some(table.wait_op(flight.tag));
        match mode {
            Mode::Inline => {
                table.file_one(answered(flight.tag));
                flight.is_handed = true;
            }
            Mode::Pending => {}
            Mode::Detached => {
                inbox.hand(caller, flight.tag);
                flight.is_handed = true;
            }
        }
        return Step::Moved;
    }

    if let Some(wait) = flight.wait.as_mut() {
        if Pin::new(wait).poll(cx).is_ready() {
            return Step::Answered;
        }
    }

    // The poll above left the waker, which is what the pending shape lands behind.
    if !flight.is_handed {
        table.file_one(answered(flight.tag));
        flight.is_handed = true;
        return Step::Moved;
    }
    Step::Stalled
}

/// Drive one caller's share of a cell, returning when its ops are done
fn drive(table: &SlotTable, mode: Mode, caller: usize, depth: usize, ops: u64, inbox: &Inbox) {
    let waker = Waker::from(Arc::new(Unparker(thread::current())));
    let mut cx = Context::from_waker(&waker);
    let mut flights: Vec<Flight> = Vec::with_capacity(depth);

    let mut issued = 0u64;
    let mut done = 0u64;
    while done < ops {
        while flights.len() < depth && issued < ops {
            flights.push(Flight {
                tag: table.next_tag(),
                wait: None,
                is_handed: false,
            });
            issued += 1;
        }

        let mut moved = false;
        let mut at = 0;
        while at < flights.len() {
            match advance(table, mode, caller, inbox, &mut flights[at], &mut cx) {
                Step::Answered => {
                    flights.swap_remove(at);
                    done += 1;
                    moved = true;
                }
                Step::Moved => {
                    moved = true;
                    at += 1;
                }
                Step::Stalled => at += 1,
            }
        }
        if !moved && !flights.is_empty() {
            thread::park();
        }
    }
}

/// Run one cell and return the ops per second it sustained
fn measure(mode: Mode, threads: usize, depth: usize, ops: u64) -> f64 {
    let table = Arc::new(SlotTable::new());
    let inbox = Arc::new(Inbox::new(threads));
    let gate = Arc::new(Barrier::new(threads + 1));

    let lander = match mode {
        Mode::Detached => {
            let table = Arc::clone(&table);
            let inbox = Arc::clone(&inbox);
            Some(thread::spawn(move || land(&table, &inbox)))
        }
        Mode::Inline | Mode::Pending => None,
    };

    let mut callers = Vec::with_capacity(threads);
    for caller in 0..threads {
        let table = Arc::clone(&table);
        let inbox = Arc::clone(&inbox);
        let gate = Arc::clone(&gate);
        callers.push(thread::spawn(move || {
            gate.wait();
            drive(&table, mode, caller, depth, ops, &inbox);
            inbox.running.fetch_sub(1, Ordering::Release);
        }));
    }

    gate.wait();
    let started = Instant::now();
    for caller in callers {
        caller.join().expect("a caller joins");
    }
    let elapsed = started.elapsed().as_secs_f64();
    if let Some(lander) = lander {
        lander.join().expect("the lander joins");
    }

    assert_eq!(
        table.outstanding(),
        0,
        "the table came back with flights on it"
    );
    assert_eq!(table.wakers(), 0, "the table came back with wakers seated");
    (threads as u64 * ops) as f64 / elapsed
}

// what one table serves as the callers on it multiply
#[test]
#[ignore]
fn report_slot_contention() {
    // libtest leaves "test name ... " open, so a header printed into it lands a
    // screen-width right of the rows underneath it.
    println!();
    let ops = env_num("REEL_SLOT_OPS", DEFAULT_OPS);
    let threads = env_list("REEL_SLOT_THREADS", DEFAULT_THREADS);
    let depths = env_list("REEL_SLOT_DEPTHS", DEFAULT_DEPTHS);
    println!("\n{ops} ops per caller thread\n");
    println!(
        "{:>10}{:>8}{:>10}{:>14}{:>10}{:>9}",
        "mode", "callers", "depth", "ops/s", "us/op", "scale"
    );

    for mode in modes() {
        for depth in &depths {
            let mut alone = 0.0;
            for threads in &threads {
                let rate = measure(mode, *threads as usize, *depth as usize, ops);
                if alone == 0.0 {
                    alone = rate;
                }
                let per_op = *threads as f64 / rate * 1e6;
                println!(
                    "{:>10}{threads:>8}{depth:>10}{rate:>14.0}{per_op:>10.2}{:>8.2}x",
                    mode.name(),
                    rate / alone,
                );
            }
        }
    }
}

// a caller that drops its futures mid flight leaves the table empty behind it
#[test]
#[ignore]
fn dropped_flights_come_back() {
    let table = SlotTable::new();
    let waker = Waker::from(Arc::new(Unparker(thread::current())));
    let mut cx = Context::from_waker(&waker);

    let mut tags = Vec::new();
    for _ in 0..64 {
        let tag = table.next_tag();
        let wanted = [tag];
        let mut claiming = table.claim(&wanted);
        assert!(Pin::new(&mut claiming).poll(&mut cx).is_ready());
        tags.push(tag);
    }
    {
        let mut waits: Vec<IoWait> = Vec::new();
        for tag in &tags {
            waits.push(table.wait_op(*tag));
        }
        for wait in waits.iter_mut() {
            assert!(Pin::new(wait).poll(&mut cx).is_pending());
        }
    }

    for tag in &tags {
        table.file_one(answered(*tag));
    }

    assert_eq!(table.outstanding(), 0);
    assert_eq!(table.wakers(), 0);
    assert_eq!(table.reclaimed(), tags.len() as u64);
}
