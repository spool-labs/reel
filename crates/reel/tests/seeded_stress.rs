//! A seeded walk over both doors, everything drawn from one number
//!
//! Caller count, residency, tail count, op mix, batch width, which door each call takes,
//! futures dropped mid-flight and the fault plan all come from one u64, so a failure is a
//! seed anyone can replay. Every stall is bounded and panics with the seed.
//!
//! What is asserted depends on what the plan drew. An honest plan keeps the strong
//! invariant: every served payload is whole and was attempted by somebody, and after a
//! flush a reopen serves exactly what the live store did. A plan that lies about
//! durability or crashes mid-walk cannot promise equality, so it holds only the
//! attempted half. DropCompletion is never drawn, since the awaited door has no give-up
//! for a completion that never comes, so a drawn drop is a hang by design.
//!
//! Knobs: REEL_STRESS_OPS and REEL_STRESS_PASSES size one walk, REEL_STRESS_SEEDS with
//! REEL_STRESS_SHARDS, REEL_STRESS_SHARD and REEL_STRESS_SKIP size and split a campaign,
//! REEL_STRESS_REPLAY, REEL_STRESS_IMAGE and REEL_STRESS_CROSS drive the replay and
//! forensics tests, and REEL_STRESS_DEBUG arms the stall watchdog.

#[allow(dead_code)]
mod harness;

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::task::{Context, Poll, Wake, Waker};
use std::thread::{self, Thread};
use std::time::{Duration, Instant};

use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};

use reel::format::footer::SegmentFooter;
use reel::format::record::{RecordHeader, HEADER_LEN};
use reel::io::fault::{FaultKind, FaultPlan};
use reel::io::sim_backend::{DurableImage, SimIo};
use reel::{
    ByteCount, IndexResidency, Preallocate, RecordKey, RecordWrite, ReelConfig, ReelStore,
    Result as ReelResult, SyncPolicy, ThreadBudget, SEGMENT_SUFFIX,
};

use harness::wire::{record_key, ID_LEN, TEST_COLUMNS};

/// One key, as the group and address a caller names it by
type Key = (u16, u8);

/// Versions each key was attempted with, recorded before the call because a future
/// dropped mid-flight never reports whether its write landed
type Attempted = BTreeMap<Key, BTreeSet<u64>>;

/// Groups the callers spread their keys across
const GROUPS: &[u16] = &[3, 4];

/// Distinct addresses per group, small enough that callers collide
const ADDRESS_SPACE: u8 = 10;

/// Shortest payload a stamped version carries
const MIN_LEN: usize = 16;

/// How much longer than the shortest a payload can be
const LEN_SPREAD: usize = 400;

/// Ops each caller draws when the environment does not override it
const OPS_PER_CALLER: u64 = 200;

/// Held futures the backlog pin keeps in flight at once, past the ring's 512 tags
const BACKLOG_WIDTH: usize = 600;

/// How long one awaited op may stand before the walk fails with its seed
const DRIVE_STALL: Duration = Duration::from_secs(30);

/// Poll rounds the backlog pin allows before failing rather than hanging
const BACKLOG_ROUNDS: u64 = 1_000_000;

/// Records kept from the end of a segment walk, for the testimony
const TESTIMONY_TAIL: usize = 4;

/// Bytes printed from where a segment walk stopped
const TESTIMONY_BYTES: usize = 64;

/// Bytes of a key printed in the forensics dump
const KEY_PREFIX_PRINTED: usize = 4;

/// Bytes of trailer a segment carries after its footer
const TRAILER_LEN: usize = 8;

fn ops_per_caller() -> u64 {
    std::env::var("REEL_STRESS_OPS")
        .ok()
        .and_then(|raw| raw.parse().ok())
        .unwrap_or(OPS_PER_CALLER)
}

/// Drawn seeds the campaign test runs, zero making it a no-op
fn campaign_seeds() -> u64 {
    std::env::var("REEL_STRESS_SEEDS")
        .ok()
        .and_then(|raw| raw.parse().ok())
        .unwrap_or(0)
}

/// How many processes share one campaign's seed stream, one meaning no sharing
fn campaign_shards() -> u64 {
    std::env::var("REEL_STRESS_SHARDS")
        .ok()
        .and_then(|raw| raw.parse().ok())
        .filter(|shards| *shards > 0)
        .unwrap_or(1)
}

/// Which slice of the stream this process walks
fn campaign_shard() -> u64 {
    std::env::var("REEL_STRESS_SHARD")
        .ok()
        .and_then(|raw| raw.parse().ok())
        .unwrap_or(0)
}

/// Seeds of this shard's own slice to pass over, for resuming a walk that died
fn campaign_skip() -> u64 {
    std::env::var("REEL_STRESS_SKIP")
        .ok()
        .and_then(|raw| raw.parse().ok())
        .unwrap_or(0)
}

/// One seed to walk over and over, named as `seed:rounds`
fn hammered() -> Option<(u64, u64)> {
    let raw = std::env::var("REEL_STRESS_HAMMER").ok()?;
    let (seed, rounds) = raw.split_once(':')?;
    Some((seed.parse().ok()?, rounds.parse().ok()?))
}

fn address(byte: u8) -> [u8; ID_LEN] {
    [byte; ID_LEN]
}

fn key_of((group, byte): Key) -> RecordKey {
    record_key(group, address(byte))
}

/// A payload whose length and every byte follow from the version in its front
fn stamped(byte: u8, version: u64) -> Vec<u8> {
    let len = MIN_LEN + (version as usize % LEN_SPREAD);
    let mut out = Vec::with_capacity(len);
    out.extend_from_slice(&version.to_le_bytes());
    out.resize(len, byte ^ (version as u8));
    out
}

fn is_stamped(byte: u8, payload: &[u8]) -> bool {
    if payload.len() < std::mem::size_of::<u64>() {
        return false;
    }
    let mut version = [0u8; std::mem::size_of::<u64>()];
    version.copy_from_slice(&payload[..std::mem::size_of::<u64>()]);
    payload == stamped(byte, u64::from_le_bytes(version)).as_slice()
}

/// Count an op error rather than expecting success, under a drawn fault plan
fn tolerated(errors: &mut u64, outcome: ReelResult<()>) {
    if outcome.is_err() {
        *errors += 1;
    }
}

fn version_of(payload: &[u8]) -> u64 {
    let mut version = [0u8; std::mem::size_of::<u64>()];
    version.copy_from_slice(&payload[..std::mem::size_of::<u64>()]);
    u64::from_le_bytes(version)
}

/// Wakes the parked driver thread, so a bounded drive sleeps rather than spins
struct ParkWaker(Thread);

impl Wake for ParkWaker {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }
}

/// Drive one future to completion, failing with the seed instead of hanging
///
/// Returns nothing when the simulator has crashed under a pending op, since a crash
/// delivers no further completions.
fn drive<Fut: Future>(seed: u64, sim: &SimIo, future: Fut) -> Option<Fut::Output> {
    let mut pinned = Box::pin(future);
    let waker = Waker::from(Arc::new(ParkWaker(thread::current())));
    let mut context = Context::from_waker(&waker);
    let deadline = Instant::now() + DRIVE_STALL;
    loop {
        match pinned.as_mut().poll(&mut context) {
            Poll::Ready(out) => return Some(out),
            Poll::Pending if sim.is_crashed() => return None,
            Poll::Pending => {
                assert!(
                    Instant::now() < deadline,
                    "seed {seed}: an awaited op stalled past {DRIVE_STALL:?}"
                );
                thread::park_timeout(Duration::from_millis(1));
            }
        }
    }
}

/// Poll a future a bounded number of times and then drop it where it stands
///
/// The waker is a no-op because the point is the drop, not the completion.
fn poll_then_drop<Fut: Future>(future: Fut, polls: u32) {
    let mut pinned = Box::pin(future);
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    for _ in 0..polls {
        if pinned.as_mut().poll(&mut context).is_ready() {
            return;
        }
    }
}

/// The shape one seed draws before anything runs
struct Shape {
    /// Caller threads the walk spawns
    callers: u64,

    /// Where the index lives
    residency: IndexResidency,

    /// Active tail threads
    tails: u32,

    /// The storage and completion faults drawn for this run
    plan: FaultPlan,

    /// How many faults that plan carries
    faults: u64,

    /// Whether the plan can lie about durability, which decides the invariant
    lies: bool,

    /// Whether the plan crashes mid-walk, after which every op simply errors
    crashes: bool,
}

/// One storage or completion fault, drawn by kind and parameter
fn drawn_fault(rng: &mut SmallRng) -> (FaultKind, bool) {
    match rng.gen_range(0..100u32) {
        // The completion plane keeps the largest share; fixed-shape suites reach it least.
        0..=39 => (
            FaultKind::DelayCompletion {
                polls: rng.gen_range(1..=4),
            },
            false,
        ),
        40..=49 => (
            FaultKind::ShortWrite {
                written_bytes: rng.gen_range(0..64),
            },
            false,
        ),
        50..=59 => (
            FaultKind::TornWrite {
                durable_bytes: rng.gen_range(0..64),
            },
            true,
        ),
        60..=66 => (FaultKind::LyingSync, true),
        67..=72 => (FaultKind::LyingSyncRange, true),
        73..=78 => (FaultKind::SyncError, true),
        79..=84 => (FaultKind::ReadError, false),
        85..=89 => (FaultKind::EnospcAppend, false),
        90..=93 => (FaultKind::EnospcAllocate, false),
        94..=96 => (FaultKind::ReorderDir, false),
        _ => (
            FaultKind::BitFlip {
                at_byte: rng.gen_range(0..32 * 1024),
                bit: rng.gen_range(0..8),
            },
            true,
        ),
    }
}

/// Draw the whole run's shape from the seed, faults included
///
/// The fault count follows the op window, so raising REEL_STRESS_OPS deepens the search
/// instead of diluting it.
fn shape_of(seed: u64) -> Shape {
    let mut rng = SmallRng::seed_from_u64(seed);
    let callers = rng.gen_range(2..=5);
    let residency = match rng.gen_range(0..10) {
        0..=2 => IndexResidency::Paged,
        _ => IndexResidency::Resident,
    };
    let tails = rng.gen_range(1..=4);

    let window = callers * ops_per_caller() * 3;
    // Faults start past the open's own ops, since one landing on the segment header
    // write fails the open the walk expects to succeed.
    let open_ops = 64;
    let mut plan = FaultPlan::new(seed);
    let mut lies = false;
    let density = rng.gen_range(50..=200);
    let faults = (window / density).max(4);
    for _ in 0..faults {
        let (kind, kind_lies) = drawn_fault(&mut rng);
        lies |= kind_lies;
        plan = plan.with_fault(rng.gen_range(open_ops..window.max(open_ops + 1)), kind);
    }
    if rng.gen_bool(0.25) {
        plan = plan.with_scatter([512, 4096][rng.gen_range(0..2)]);
        lies = true;
    }
    let crashes = rng.gen_bool(0.2);
    if crashes {
        // Late in the window, so the walk has a store worth crashing.
        plan = plan.with_crash(rng.gen_range(window / 2..window));
        lies = true;
    }

    Shape {
        callers,
        residency,
        tails,
        plan,
        faults,
        lies,
        crashes,
    }
}

/// Compaction passes admitted at once, one ticker driven per admitted pass
fn stress_passes() -> u32 {
    std::env::var("REEL_STRESS_PASSES")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(1)
}

fn config(shape: &Shape) -> ReelConfig {
    ReelConfig {
        segment_bytes: ByteCount::from_bytes(16 * 1024),
        alloc_chunk: ByteCount::from_bytes(16 * 1024),
        preallocate: Preallocate::Chunk,
        sync: SyncPolicy::Never,
        active_tails: ThreadBudget::threads(shape.tails),
        index: shape.residency,
        compact_dead_ratio: 0.1,
        // A lying plan can leave torn bytes where the index points; verification is what
        // turns serving them into the designed miss.
        verify_reads: shape.lies,
        ..ReelConfig::default()
    }
}

/// One caller's walk, its own rng stream split off the run's seed
///
/// Op errors are counted rather than expected, since under a drawn fault plan an error is
/// a legitimate outcome; the error budget asserted afterwards is what keeps tolerance
/// from hiding a storm.
fn caller_walk(store: &Arc<ReelStore>, sim: &SimIo, seed: u64, caller: u64) -> (Attempted, u64) {
    let mut rng = SmallRng::seed_from_u64(seed ^ (caller << 32) ^ 0x9E37_79B9);
    let mut attempted = Attempted::new();
    let mut errors = 0u64;
    for step in 0..ops_per_caller() {
        // A crashed simulator answers nothing more, and a dead process asks nothing more.
        if sim.is_crashed() {
            break;
        }
        let group = GROUPS[rng.gen_range(0..GROUPS.len())];
        let byte = rng.gen_range(0..ADDRESS_SPACE);
        let version = (caller << 32) | step;
        let awaited = rng.gen_bool(0.5);
        match rng.gen_range(0..100u32) {
            // A single put through the drawn door, sometimes abandoned mid-flight.
            0..=54 => {
                attempted.entry((group, byte)).or_default().insert(version);
                let key = key_of((group, byte));
                let payload = stamped(byte, version);
                // Drawn before the branch, so either path consumes the same rng stream.
                let drop_polls = rng.gen_range(1..=2);
                let drops = rng.gen_bool(0.15);
                if awaited {
                    if drops {
                        poll_then_drop(store.put_owned_wait(&key, payload), drop_polls);
                    } else {
                        match drive(seed, sim, store.put_owned_wait(&key, payload)) {
                            Some(outcome) => tolerated(&mut errors, outcome),
                            None => errors += 1,
                        }
                    }
                } else {
                    tolerated(&mut errors, store.put_owned(&key, payload));
                }
            }
            // A batch of puts and deletes, one durability point, both doors
            55..=64 => {
                let width = rng.gen_range(2..=4);
                let mut writes = Vec::with_capacity(width);
                for member in 0..width as u64 {
                    let byte = rng.gen_range(0..ADDRESS_SPACE);
                    let version = (caller << 32) | (step + (member << 24));
                    if rng.gen_bool(0.8) {
                        attempted.entry((group, byte)).or_default().insert(version);
                        writes.push(RecordWrite::Put {
                            key: key_of((group, byte)),
                            payload: stamped(byte, version),
                        });
                    } else {
                        writes.push(RecordWrite::Delete {
                            key: key_of((group, byte)),
                        });
                    }
                }
                if awaited {
                    match drive(seed, sim, store.apply_batch_wait(writes)) {
                        Some(outcome) => tolerated(&mut errors, outcome),
                        None => errors += 1,
                    }
                } else {
                    tolerated(&mut errors, store.apply_batch(writes));
                }
            }
            65..=74 => {
                tolerated(&mut errors, store.delete(&key_of((group, byte))));
            }
            // A read of one key, either door, checked for a torn or foreign answer
            75..=89 => {
                let key = key_of((group, byte));
                let answer = if awaited {
                    match drive(seed, sim, store.get_wait(&key)) {
                        Some(answer) => answer,
                        None => continue,
                    }
                } else {
                    store.get(&key)
                };
                match answer {
                    Ok(Some(value)) => {
                        assert!(
                            is_stamped(byte, &value),
                            "seed {seed}: group {group} key {byte} served torn"
                        );
                    }
                    Ok(None) => {}
                    Err(_) => errors += 1,
                }
            }
            // A spanning read, which is what the publish barrier answers for
            90..=97 => {
                let keys: Vec<_> = (0..ADDRESS_SPACE)
                    .map(|byte| key_of((group, byte)))
                    .collect();
                let answers = if awaited {
                    match drive(seed, sim, store.get_many_wait(&keys)) {
                        Some(answers) => answers,
                        None => continue,
                    }
                } else {
                    store.get_many(&keys)
                };
                match answers {
                    Ok(answers) => {
                        for (byte, answer) in answers.iter().enumerate() {
                            if let Some(value) = answer {
                                assert!(
                                    is_stamped(byte as u8, value),
                                    "seed {seed}: group {group} key {byte} torn in a batch read"
                                );
                            }
                        }
                    }
                    Err(_) => errors += 1,
                }
            }
            _ => {
                if awaited {
                    match drive(seed, sim, store.flush_wait()) {
                        Some(outcome) => tolerated(&mut errors, outcome),
                        None => errors += 1,
                    }
                } else {
                    tolerated(&mut errors, store.flush());
                }
            }
        }
    }
    (attempted, errors)
}

/// Everything the store serves over the key space, errors tolerated per key
/// Versions a second life stamps, past anything a caller walk can draw
const SECOND_LIFE: u64 = 1 << 48;

/// A second life on the walk's image: the open resumes the tails, a quarter of
/// a walk lands behind the resumed rows, and a third open must hand back
/// exactly what the second held. The device is fault free, so nothing is
/// waived, whatever shape the first life took.
fn resumed_life(seed: u64, shape: &Shape, root: PathBuf, image: DurableImage) {
    let sim = SimIo::from_image(image);
    let store =
        ReelStore::open_with_io(root.clone(), config(shape), TEST_COLUMNS, Arc::new(sim.clone()))
            .expect("a resumed open");
    let mut rng = SmallRng::seed_from_u64(seed ^ 0x5EC0_11FE);
    let mut expected: BTreeMap<Key, u64> = BTreeMap::new();
    for step in 0..(ops_per_caller() / 4 + 16) {
        let group = GROUPS[rng.gen_range(0..GROUPS.len())];
        let byte = rng.gen_range(0..ADDRESS_SPACE);
        let key = (group, byte);
        if rng.gen_bool(0.85) {
            let version = SECOND_LIFE | step;
            store
                .put(&key_of(key), &stamped(byte, version))
                .expect("a second life put");
            expected.insert(key, version);
        } else {
            store.delete(&key_of(key)).expect("a second life delete");
            expected.remove(&key);
        }
    }
    store.flush().expect("a second life flush");
    let (held, failed) = view(&store);
    assert!(
        failed.is_empty(),
        "seed {seed}: a resumed store failed to read {failed:?}"
    );
    for (key, version) in &expected {
        let payload = held
            .get(key)
            .unwrap_or_else(|| panic!("seed {seed}: a resumed store lost {key:?}"));
        assert_eq!(
            version_of(payload),
            *version,
            "seed {seed}: a resumed store serves a stale version for {key:?}"
        );
    }
    drop(store);

    let third = ReelStore::open_with_io(
        root,
        config(shape),
        TEST_COLUMNS,
        Arc::new(SimIo::from_image(sim.durable_image())),
    )
    .expect("a third open");
    let (after, failed) = view(&third);
    assert!(
        failed.is_empty(),
        "seed {seed}: a third open failed to read {failed:?}"
    );
    assert_eq!(
        held, after,
        "seed {seed}: a resume cycle changed what the store holds"
    );
}

fn view(store: &ReelStore) -> (BTreeMap<Key, Vec<u8>>, BTreeSet<Key>) {
    let mut held = BTreeMap::new();
    let mut failed = BTreeSet::new();
    for group in GROUPS {
        for byte in 0..ADDRESS_SPACE {
            let key = (*group, byte);
            match store.get(&key_of(key)) {
                Ok(Some(value)) => {
                    held.insert(key, value.into_vec());
                }
                Ok(None) => {}
                // A read that failed is not a key that is absent; folding the two
                // together reads a fault landing on the check as a disagreement.
                Err(_) => {
                    failed.insert(key);
                }
            }
        }
    }
    (held, failed)
}

/// Assert one side serves only whole payloads carrying attempted versions
fn assert_attempted(seed: u64, side: &str, held: &BTreeMap<Key, Vec<u8>>, attempted: &Attempted) {
    for ((group, byte), payload) in held {
        assert!(
            is_stamped(*byte, payload),
            "seed {seed}: {side} serves group {group} key {byte} torn"
        );
        let versions = attempted
            .get(&(*group, *byte))
            .unwrap_or_else(|| panic!("seed {seed}: {side} serves a key nobody attempted"));
        assert!(
            versions.contains(&version_of(payload)),
            "seed {seed}: {side} serves group {group} key {byte} with a version nobody attempted"
        );
    }
}

/// Run one drawn shape to completion and hold it to the invariant it earns
fn walk(seed: u64) {
    let shape = shape_of(seed);
    // Captured output only surfaces when a seed fails, which is when the shape is wanted.
    println!(
        "seed {seed}: {} callers, {:?}, {} tails, {} faults, lies {}, crashes {}",
        shape.callers, shape.residency, shape.tails, shape.faults, shape.lies, shape.crashes
    );
    let sim = SimIo::new(shape.plan.clone());
    let root = PathBuf::from("/walk");
    let store = Arc::new(
        ReelStore::open_with_io(
            root.clone(),
            config(&shape),
            TEST_COLUMNS,
            Arc::new(sim.clone()),
        )
        .expect("open"),
    );

    let is_running = Arc::new(AtomicBool::new(true));
    // Callers plus one ticker per admitted pass; sizing this short leaves the extras
    // waiting on a barrier that has already released, and the run hangs doing nothing.
    let barrier = Arc::new(Barrier::new(
        shape.callers as usize + stress_passes().max(1) as usize,
    ));
    let mut handles = Vec::new();
    for caller in 0..shape.callers {
        let store = Arc::clone(&store);
        let sim = sim.clone();
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            barrier.wait();
            caller_walk(&store, &sim, seed, caller)
        }));
    }
    // Maintenance races the callers, and the flush is the pump: the simulator moves
    // completions only when something polls it. One ticker per admitted pass, since a
    // single ticker can never have two rewrites under way at once.
    let tickers: Vec<_> = (0..stress_passes().max(1))
        .map(|_| {
            let store = Arc::clone(&store);
            let is_running = Arc::clone(&is_running);
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                let deadline = Instant::now() + 4 * DRIVE_STALL;
                while is_running.load(Ordering::Acquire) && Instant::now() < deadline {
                    let _ = store.maintain_once();
                    let _ = store.flush();
                    thread::yield_now();
                }
            })
        })
        .collect();

    // Dumps both sides of the completion seam before the drive deadline fails the walk.
    let watchdog = std::env::var("REEL_STRESS_DEBUG").is_ok().then(|| {
        let store = Arc::clone(&store);
        let sim = sim.clone();
        let is_running = Arc::clone(&is_running);
        thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(15);
            while is_running.load(Ordering::Acquire) {
                if Instant::now() > deadline {
                    eprintln!("sim: {}", sim.debug_counts());
                    eprintln!("slots:\n{}", store.debug_io());
                    return;
                }
                thread::park_timeout(Duration::from_millis(200));
            }
        })
    });

    let mut attempted = Attempted::new();
    let mut errors = 0u64;
    for handle in handles {
        let (theirs, their_errors) = handle.join().expect("caller");
        for (key, versions) in theirs {
            attempted.entry(key).or_default().extend(versions);
        }
        errors += their_errors;
    }
    is_running.store(false, Ordering::Release);
    for ticker in tickers {
        ticker.join().expect("ticker");
    }
    if let Some(watchdog) = watchdog {
        watchdog.thread().unpark();
        let _ = watchdog.join();
    }
    // The plan belongs to the walk, not to the check that follows it: the reopen reads a
    // fault free device, so leaving faults armed here asks one side to answer under a
    // plan the other never sees. The reach line reports the tail a short walk never got
    // to, which thins the search by a fraction nothing else shows.
    let (fired, drawn) = sim.fault_reach();
    println!("seed {seed}: faults reached {fired} of {drawn}");
    sim.disarm();

    // Only an uncrashed walk can bound its errors by its faults. The factor is slack for
    // the cascade a broken segment causes, not a derived number.
    if !shape.crashes {
        assert!(
            errors <= shape.faults * 8 + 8,
            "seed {seed}: {errors} op errors from {} faults reads as an error storm",
            shape.faults
        );
    }

    // Only a walk whose closing flush succeeded may demand the reopen equal the live
    // store: a failed sync marks its segment broken for good, so the records it never
    // covered are cached but not durable, and the difference is the durability model.
    let flushed = !shape.crashes && store.flush().is_ok();
    if !shape.crashes && !flushed {
        println!("seed {seed}: the closing flush failed, durability equality waived");
    }
    let live = (!shape.crashes).then(|| {
        let (live, failed) = view(&store);
        assert!(
            shape.lies || failed.is_empty(),
            "seed {seed}: the live store failed to read {failed:?} with the plan disarmed"
        );
        assert_attempted(seed, "the live store", &live, &attempted);
        // Taken while the live store still stands, so a mismatch after reopen can name
        // its mechanism.
        let mut sites = BTreeMap::new();
        let mut lsns = BTreeMap::new();
        for group in GROUPS {
            for byte in 0..ADDRESS_SPACE {
                let key = (*group, byte);
                if let Ok(found) = store.index().sites(&key_of(key)) {
                    sites.insert(key, found);
                }
                if let Ok(Some(entry)) = store.index().get(&key_of(key)) {
                    lsns.insert(key, entry.lsn.as_u64());
                }
            }
        }
        (live, sites, lsns)
    });
    // Whether the live store would search a segment the volume no longer has. A candidate
    // the device cannot show is one compaction retired, and a read picking it answers as
    // though the key had never been written. A crashed plan never reaches this, since a
    // crash is free to take a file the index still names.
    if let Some((_, sites, _)) = live.as_ref() {
        let standing: BTreeSet<u32> = sim
            .durable_image()
            .iter()
            .filter_map(|(path, _)| segment_number_of(path))
            .collect();
        let mut phantom = BTreeSet::new();
        for found in sites.values() {
            for segment in &found.candidates {
                if !standing.contains(&segment.as_u32()) {
                    phantom.insert(segment.as_u32());
                }
            }
        }
        assert!(
            phantom.is_empty(),
            "seed {seed}: the live store still searches retired segments {phantom:?}, standing {standing:?}"
        );
    }
    drop(store);

    let image = sim.durable_image();
    let restored = SimIo::from_image(image.clone());
    let reopened =
        ReelStore::open_with_io(root.clone(), config(&shape), TEST_COLUMNS, Arc::new(restored))
            .expect("reopen");
    let (after, after_failed) = view(&reopened);
    assert!(
        shape.lies || after_failed.is_empty(),
        "seed {seed}: a reopen failed to read {after_failed:?} from a fault free device"
    );
    assert_attempted(seed, "a reopen", &after, &attempted);
    resumed_life(seed, &shape, root.clone(), image.clone());
    if let Some((live, live_sites, live_lsns)) = live {
        if !shape.lies && flushed && live != after {
            // The index's own testimony for the keys the two sides disagree on, which is
            // what turns a mismatch into a mechanism.
            let mut testimony = String::new();
            let mut real = 0usize;
            for key in live.keys().chain(after.keys()) {
                let (here, there) = (live.get(key), after.get(key));
                if here.map(|payload| version_of(payload))
                    == there.map(|payload| version_of(payload))
                {
                    continue;
                }
                // A put can error after its bytes landed, so a reopen resolving a strictly
                // newer attempted version is the durability model speaking. Anything
                // older or unattempted stays a bug.
                let resurrected = errors > 0
                    && there.is_some_and(|payload| {
                        attempted
                            .get(key)
                            .is_some_and(|versions| versions.contains(&version_of(payload)))
                    })
                    && {
                        let reopen_lsn = reopened
                            .index()
                            .get(&key_of(*key))
                            .ok()
                            .flatten()
                            .map(|entry| entry.lsn.as_u64());
                        match (live_lsns.get(key), reopen_lsn) {
                            (Some(old), Some(new)) => new > *old,
                            _ => false,
                        }
                    };
                if resurrected {
                    println!(
                        "seed {seed}: a reopen resurrected an errored write for {key:?}, waived"
                    );
                    continue;
                }
                real += 1;
                testimony.push_str(&format!(
                    "key {key:?}: live {:?} reopen {:?}\nlive sites: {:?}\nreopen sites: {:?}\n",
                    here.map(|payload| version_of(payload)),
                    there.map(|payload| version_of(payload)),
                    live_sites.get(key),
                    reopened.index().sites(&key_of(*key)),
                ));
            }
            if real == 0 {
                return;
            }
            // The reopen is deterministic given the image, so saving it turns a
            // one-in-thousands interleaving into a repeatable case.
            let keep =
                std::env::temp_dir().join(format!("seeded-image-{seed}-{}", std::process::id()));
            let _ = std::fs::create_dir_all(&keep);
            for (path, bytes) in image.iter() {
                if let Some(name) = path.file_name() {
                    let _ = std::fs::write(keep.join(name), bytes);
                }
            }
            testimony.push_str(&format!("image kept at {}\n", keep.display()));
            for (path, bytes) in image.iter() {
                if path.to_string_lossy().ends_with(SEGMENT_SUFFIX) {
                    testimony.push_str(&scan_segment(path, bytes));
                }
            }
            panic!("seed {seed}: a reopen disagrees with the live store\n{testimony}");
        }
    }
}

/// The number of the segment this path names, for a file in a kept image
fn segment_number_of(path: &Path) -> Option<u32> {
    path.file_name()?
        .to_str()?
        .strip_suffix(SEGMENT_SUFFIX)?
        .parse()
        .ok()
}

/// Walk one segment's raw records, for the testimony when a reopen disagrees
///
/// Where the walk stops is the mechanism: the offset, the bytes standing there and the
/// last records reached say whether a range was lost, stamped over, or torn.
fn scan_segment(path: &Path, bytes: &[u8]) -> String {
    let mut out = format!(
        "segment {:?}, {} bytes:\n",
        path.file_name().unwrap_or_default(),
        bytes.len()
    );
    let mut at = 0usize;
    let mut walked = 0usize;
    let mut tail: VecDeque<String> = VecDeque::with_capacity(TESTIMONY_TAIL);
    while at + HEADER_LEN <= bytes.len() {
        let Ok(header) = RecordHeader::unpack(&bytes[at..]) else {
            out.push_str(&format!("  walk stopped at {at}: header does not parse\n"));
            break;
        };
        if header.is_unwritten() {
            out.push_str(&format!("  walk stopped at {at}: unwritten header\n"));
            break;
        }
        let span = header.span() as usize;
        if tail.len() == TESTIMONY_TAIL {
            tail.pop_front();
        }
        tail.push_back(format!(
            "at {at} lsn {} span {span} {:?}",
            header.lsn.as_u64(),
            header.flags
        ));
        walked += 1;
        if span == 0 || at + span > bytes.len() {
            out.push_str(&format!(
                "  walk stopped at {at}: span {span} runs out of the file\n"
            ));
            break;
        }
        at += span;
    }
    out.push_str(&format!("  {walked} records, reached {at}\n"));
    for line in &tail {
        out.push_str(&format!("  {line}\n"));
    }
    let stop = at.min(bytes.len());
    let end = (stop + TESTIMONY_BYTES).min(bytes.len());
    out.push_str(&format!("  bytes at {stop}: {:02x?}\n", &bytes[stop..end]));
    out
}

macro_rules! drawn_shape {
    ($name:ident, $seed:expr) => {
        #[test]
        fn $name() {
            walk($seed);
        }
    };
}

drawn_shape!(drawn_shape_seed_1, 1);
drawn_shape!(drawn_shape_seed_2, 2);
drawn_shape!(drawn_shape_seed_3, 3);
drawn_shape!(drawn_shape_seed_5, 5);
drawn_shape!(drawn_shape_seed_8, 8);
drawn_shape!(drawn_shape_seed_13, 13);

// a reopen must not resurrect a deleted key: the seed a campaign caught compaction on,
// dropping a tombstone while a number drawn before it had still to land
drawn_shape!(
    a_reopen_must_not_resurrect_a_deleted_key,
    11630724910943363631
);

// replay one drawn seed by number, the knob a campaign failure names
#[test]
fn replay() {
    if let Ok(raw) = std::env::var("REEL_STRESS_REPLAY") {
        walk(raw.parse().expect("a seed"));
    }
}

// walk one drawn seed over and over, for a race a single pass almost never draws
//
// A walk is threaded, so one replay of a seed says nothing about an interleaving.
// Seed 1696173307150234702 parts the live store from a reopen about once in a few
// thousand rounds, and does so at the pre-performance tip as well.
//
// Opt in with REEL_STRESS_HAMMER=<seed>:<rounds>.
#[test]
fn hammer() {
    let Some((seed, rounds)) = hammered() else {
        return;
    };
    println!("hammering seed {seed} for {rounds} rounds");
    for round in 0..rounds {
        if round % 500 == 0 {
            println!("round {round}");
        }
        walk(seed);
    }
}

// reopen a kept image under its seed's shape and print what every key answers
#[test]
fn forensics() {
    let (Ok(dir), Ok(raw)) = (
        std::env::var("REEL_STRESS_IMAGE"),
        std::env::var("REEL_STRESS_REPLAY"),
    ) else {
        return;
    };
    let seed: u64 = raw.parse().expect("a seed");
    let shape = shape_of(seed);
    let mut image: DurableImage = Vec::new();
    for entry in std::fs::read_dir(&dir).expect("image dir") {
        let entry = entry.expect("entry");
        let bytes = std::fs::read(entry.path()).expect("image file");
        image.push((PathBuf::from("/walk").join(entry.file_name()), bytes));
    }
    image.sort_by(|a, b| a.0.cmp(&b.0));

    let restored = SimIo::from_image(image);
    let store = ReelStore::open_with_io(
        PathBuf::from("/walk"),
        config(&shape),
        TEST_COLUMNS,
        Arc::new(restored),
    )
    .expect("reopen");
    for group in GROUPS {
        for byte in 0..ADDRESS_SPACE {
            let key = (*group, byte);
            if let Ok(Some(value)) = store.get(&key_of(key)) {
                println!("({group}, {byte}) -> {}", version_of(&value));
            }
            if let Ok(sites) = store.index().sites(&key_of(key)) {
                if sites.resident.is_some() || !sites.sealed.is_empty() {
                    println!("  sites {key:?}: {sites:?}");
                }
            }
        }
    }

    if let Ok(pick) = std::env::var("REEL_STRESS_CROSS") {
        let bytes = std::fs::read(Path::new(&dir).join(&pick)).expect("segment");
        let trailer = &bytes[bytes.len() - TRAILER_LEN..];
        let footer_len = u32::from_le_bytes(trailer[..4].try_into().expect("len")) as usize;
        let footer = SegmentFooter::parse(&bytes[bytes.len() - footer_len..]).expect("footer");
        println!("cross-check {pick}: records first");
        let region = bytes.len() - footer_len;
        let mut at = 0usize;
        while at + HEADER_LEN <= region {
            let Ok(header) = RecordHeader::unpack(&bytes[at..]) else {
                break;
            };
            if header.is_unwritten() {
                break;
            }
            if header.flags.is_data() || header.flags.is_tombstone() {
                println!(
                    "  record at {at}: lsn {} key {:02x?}",
                    header.lsn.as_u64(),
                    &header.key.as_slice()[..KEY_PREFIX_PRINTED.min(header.key.as_slice().len())]
                );
            }
            let span = header.span() as usize;
            if span == 0 || at + span > bytes.len() {
                break;
            }
            at += span;
        }
        println!("footer rows:");
        for partition in &footer.partitions {
            for row in partition.entries() {
                let row = row.expect("row");
                println!(
                    "  row key {:02x?} lsn {} offset {} len {}",
                    &row.key.as_slice()[..KEY_PREFIX_PRINTED.min(row.key.as_slice().len())],
                    row.lsn.as_u64(),
                    row.offset,
                    row.len
                );
            }
        }
    }

    println!("footer census:");
    for entry in std::fs::read_dir(&dir).expect("image dir") {
        let entry = entry.expect("entry");
        let bytes = std::fs::read(entry.path()).expect("image file");
        if bytes.len() < TRAILER_LEN {
            continue;
        }
        let trailer = &bytes[bytes.len() - TRAILER_LEN..];
        let footer_len = u32::from_le_bytes(trailer[..4].try_into().expect("len")) as usize;
        if footer_len == 0 || footer_len > bytes.len() {
            println!("  {:?}: no footer", entry.file_name());
            continue;
        }
        match SegmentFooter::parse(&bytes[bytes.len() - footer_len..]) {
            Ok(footer) => {
                let shape: Vec<(u8, usize)> = footer
                    .partitions
                    .iter()
                    .map(|part| (part.column.as_u8(), part.len()))
                    .collect();
                println!("  {:?}: partitions {shape:?}", entry.file_name());
            }
            Err(error) => println!("  {:?}: footer does not parse: {error}", entry.file_name()),
        }
    }
}

// a campaign of drawn seeds, a no-op unless the environment sizes it
#[test]
fn campaign() {
    let mut rng = SmallRng::seed_from_u64(0xC0FFEE);
    let shards = campaign_shards();
    let mine = campaign_shard() % shards;
    let skip = campaign_skip();
    let mut drawn = 0u64;
    for index in 0..campaign_seeds() {
        let seed: u64 = rng.gen();
        if index % shards != mine {
            continue;
        }
        drawn += 1;
        if drawn > skip {
            walk(seed);
        }
    }
}

// a lone awaited read progresses with no blocking traffic to ride on, no pump here
#[test]
fn a_lone_awaited_read_needs_no_pump() {
    let sim = SimIo::new(FaultPlan::new(7));
    let shape = Shape {
        callers: 1,
        residency: IndexResidency::Resident,
        tails: 1,
        plan: FaultPlan::new(0),
        faults: 0,
        lies: false,
        crashes: false,
    };
    let store = Arc::new(
        ReelStore::open_with_io(
            PathBuf::from("/lone"),
            config(&shape),
            TEST_COLUMNS,
            Arc::new(sim.clone()),
        )
        .expect("open"),
    );

    store
        .put_owned(&key_of((3, 1)), stamped(1, 9))
        .expect("put");
    let value = drive(7, &sim, store.get_wait(&key_of((3, 1))))
        .expect("nothing crashes here")
        .expect("get")
        .expect("present");
    assert!(is_stamped(1, &value), "the lone awaited read settled torn");
}

// four callers filing awaited puts at once must not turn a claim collision into an error
#[test]
fn inline_callers_colliding_on_claims() {
    let mut plan = FaultPlan::new(0x1234);
    for at in (0..2_000u64).step_by(5) {
        plan = plan.with_fault(at, FaultKind::DelayCompletion { polls: 2 });
    }
    let sim = SimIo::new(plan);
    let sim_for_callers = sim.clone();
    let root = PathBuf::from("/claims");
    let shape = Shape {
        callers: 4,
        residency: IndexResidency::Resident,
        tails: 1,
        plan: FaultPlan::new(0),
        faults: 0,
        lies: false,
        crashes: false,
    };
    let store = Arc::new(
        ReelStore::open_with_io(root, config(&shape), TEST_COLUMNS, Arc::new(sim)).expect("open"),
    );

    let is_running = Arc::new(AtomicBool::new(true));
    // The pump: blocking polls are what move the simulator's completions.
    let pump = {
        let store = Arc::clone(&store);
        let is_running = Arc::clone(&is_running);
        thread::spawn(move || {
            let deadline = Instant::now() + 4 * DRIVE_STALL;
            while is_running.load(Ordering::Acquire) && Instant::now() < deadline {
                let _ = store.flush();
                thread::yield_now();
            }
        })
    };
    let barrier = Arc::new(Barrier::new(4));
    let mut handles = Vec::new();
    for caller in 0..4u64 {
        let store = Arc::clone(&store);
        let sim = sim_for_callers.clone();
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            barrier.wait();
            for step in 0..400u64 {
                let byte = (step % ADDRESS_SPACE as u64) as u8;
                let version = (caller << 32) | step;
                drive(
                    0x1234,
                    &sim,
                    store.put_owned_wait(&key_of((3, byte)), stamped(byte, version)),
                )
                .expect("nothing crashes here")
                .expect("awaited put");
            }
        }));
    }
    for handle in handles {
        handle.join().expect("caller");
    }
    is_running.store(false, Ordering::Release);
    pump.join().expect("pump");
    store.flush().expect("flush");
    for byte in 0..ADDRESS_SPACE {
        let value = store
            .get(&key_of((3, byte)))
            .expect("get")
            .expect("present");
        assert!(is_stamped(byte, &value), "key {byte} settled torn");
    }
}

// a backlog of held futures far past the ring's tag table must all complete, none starved
#[test]
fn a_backlog_of_futures_past_the_tag_table() {
    let mut plan = FaultPlan::new(0x0512);
    for at in (0..BACKLOG_WIDTH as u64 * 4).step_by(2) {
        plan = plan.with_fault(at, FaultKind::DelayCompletion { polls: 3 });
    }
    let sim = SimIo::new(plan);
    let root = PathBuf::from("/backlog");
    let shape = Shape {
        callers: 1,
        residency: IndexResidency::Resident,
        tails: 2,
        plan: FaultPlan::new(0),
        faults: 0,
        lies: false,
        crashes: false,
    };
    let store = Arc::new(
        ReelStore::open_with_io(root, config(&shape), TEST_COLUMNS, Arc::new(sim)).expect("open"),
    );

    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    let mut in_flight: Vec<Pin<Box<dyn Future<Output = ReelResult<()>>>>> = Vec::new();
    for step in 0..BACKLOG_WIDTH as u64 {
        let byte = (step % ADDRESS_SPACE as u64) as u8;
        let payload = stamped(byte, step);
        let key = key_of((4, byte));
        let store = Arc::clone(&store);
        in_flight.push(Box::pin(async move {
            store.put_owned_wait(&key, payload).await
        }));
    }
    // Round-robin polling keeps the whole backlog in flight at once, the flush between
    // rounds is the pump, and the round bound turns a starved future into a report
    // rather than a hang.
    let mut rounds = 0u64;
    while !in_flight.is_empty() {
        rounds += 1;
        assert!(
            rounds < BACKLOG_ROUNDS,
            "a backlog future starved: {} still in flight after {rounds} rounds",
            in_flight.len()
        );
        in_flight.retain_mut(|future| match future.as_mut().poll(&mut context) {
            Poll::Ready(outcome) => {
                outcome.expect("backlog put");
                false
            }
            Poll::Pending => true,
        });
        store.flush().expect("pump");
    }

    store.flush().expect("flush");
    for byte in 0..ADDRESS_SPACE {
        let value = store
            .get(&key_of((4, byte)))
            .expect("get")
            .expect("present");
        assert!(is_stamped(byte, &value), "key {byte} settled torn");
    }
}
