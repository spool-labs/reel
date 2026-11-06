//! Async reads through the completion ring: each answers as its blocking twin does, on the caller's task

use std::future::Future;
use std::path::PathBuf;
use std::pin::{Pin, pin};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Wake, Waker};

use reel::config::{ColumnConfig, KeyShape, StoreConfig, SyncPolicy, VolumeConfig};
use reel::engine::reel_store::ReelStore;
use reel::engine::scan_state::MOST_SCANS;
use reel::io::ring::{RING_READS, RING_SLOT_BYTES, RING_SLOTS, ReadRing};
use reel::record::codec::Codec;
use reel_api::column_id::ColumnId;
use reel_api::direction::Direction;
use reel_api::reads::{Reads, ValueWindow};
use reel_api::scan::Scan;
use reel_api::store_error::StoreError;
use reel_api::units::{ByteCount, Nanoseconds, VolumeId};
use reel_api::value_list::{ValueAnswer, ValueList};
use reel_sim::seeded_random::Seed;
use reel_sim::simulated_io::SimulatedIo;
use reel_sim::simulated_ring::SimulatedRing;

use crate::counting::counted;

const ROOT: &str = "/disk/ring";
const COLUMN: ColumnId = ColumnId(1);
const BUDGET: ByteCount = ByteCount(96 * 1024 * 1024);
const TICK: Nanoseconds = Nanoseconds(1_000_000);
const STEPS: u64 = 400_000;
const KEYS: u64 = 3_000;
const MANY_READS: u64 = 3 * RING_SLOTS as u64;

type Store2 = ReelStore<SimulatedIo>;
type PendingRead<'call> = Pin<Box<dyn Future<Output = Result<bool, StoreError>> + Send + 'call>>;

fn open(io: &SimulatedIo) -> Store2 {
    let config = StoreConfig {
        memory_budget: BUDGET,
        threads: reel::config::NO_WORKERS,
        volumes: vec![VolumeConfig {
            name: "ring".to_string(),
            roots: vec![PathBuf::from(ROOT)],
            sync: SyncPolicy::Never,
            device: Default::default(),
        }],
        columns: vec![ColumnConfig {
            id: COLUMN,
            name: "column".to_string(),
            key_shape: KeyShape::Variable { most: 64 },
            codec: Codec::None,
            volume: VolumeId(0),
            complete_rows: None,
        }],
    };
    ReelStore::open(io.clone(), &config).expect("open")
}

fn step(io: &SimulatedIo) {
    if io.step_any().is_none() {
        io.advance_clock(TICK);
    }
}

fn settle(io: &SimulatedIo) {
    for _step in 0..STEPS {
        if io.step_any().is_none() {
            io.advance_clock(TICK);
            if io.step_any().is_none() {
                return;
            }
        }
    }
    panic!("jobs never went quiet");
}

fn run<Answer>(io: &SimulatedIo, future: impl Future<Output = Answer>) -> Answer {
    let mut future = pin!(future);
    let mut context = Context::from_waker(Waker::noop());
    for _step in 0..STEPS {
        if let Poll::Ready(answer) = future.as_mut().poll(&mut context) {
            return answer;
        }
        step(io);
    }
    panic!("a read never finished");
}

fn key(number: u64) -> Vec<u8> {
    format!("key-{number:05}").into_bytes()
}

fn value(number: u64) -> Vec<u8> {
    format!("value-{number:05}").into_bytes()
}

fn ringed(seed: u64) -> (SimulatedIo, Store2) {
    let io = SimulatedIo::new(Seed(seed));
    io.use_ring();
    let store = open(&io);
    for number in 0..KEYS {
        store.put(COLUMN, &key(number), &value(number)).expect("put");
    }
    store.seal_now();
    settle(&io);
    // a fresh open has nothing cached, so reads go to the device
    store.close().expect("close");
    drop(store);
    let store = open(&io);
    (io, store)
}

fn ring(store: &Store2) -> &ReadRing<SimulatedRing> {
    store.state.ring.as_ref().expect("a ring")
}

// a get waits on the ring with no thread held, and comes back with what the blocking call reads
#[test]
fn get_waits_on_the_ring() {
    let (io, store) = ringed(1);
    let name = key(7);
    let mut buf = Vec::new();
    {
        let mut read = pin!(Reads::get_async(&store, COLUMN, &name, &mut buf));
        let mut context = Context::from_waker(Waker::noop());
        assert!(read.as_mut().poll(&mut context).is_pending());
        for _step in 0..STEPS {
            if let Poll::Ready(found) = read.as_mut().poll(&mut context) {
                assert!(found.expect("get"));
                break;
            }
            step(&io);
        }
    }
    assert_eq!(buf, value(7));
    store.close().expect("close");
}

// every form answers as the blocking calls do: found, missing and a window
#[test]
fn every_form_answers() {
    let (io, store) = ringed(2);
    store.put(COLUMN, b"open", b"open value").expect("put");
    let mut buf = Vec::new();
    for number in (0..KEYS).step_by(97) {
        assert!(run(&io, Reads::get_async(&store, COLUMN, &key(number), &mut buf)).expect("get"));
        assert_eq!(buf, value(number));
    }
    assert!(run(&io, Reads::get_async(&store, COLUMN, b"open", &mut buf)).expect("get"));
    assert_eq!(buf, b"open value");
    assert!(!run(&io, Reads::get_async(&store, COLUMN, b"missing", &mut buf)).expect("get"));
    let window = ValueWindow { offset: ByteCount(6), length: ByteCount(5) };
    assert!(run(&io, Reads::get_range_async(&store, COLUMN, &key(7), window, &mut buf)).expect("window"));
    assert_eq!(buf, b"00007");
    store.close().expect("close");
}

// a value larger than a slot comes through in turns
#[test]
fn large_value_in_turns() {
    let (io, store) = ringed(3);
    let large: Vec<u8> = (0..3 * RING_SLOT_BYTES + 17).map(|at| (at % 251) as u8).collect();
    store.put(COLUMN, b"large", &large).expect("put");
    store.seal_now();
    settle(&io);
    let mut buf = Vec::new();
    assert!(run(&io, Reads::get_async(&store, COLUMN, b"large", &mut buf)).expect("get"));
    assert_eq!(buf, large);
    store.close().expect("close");
}

// a read dropped with the device working leaves its slot to the completion, and every slot comes back
#[test]
fn dropped_read_frees_its_slot() {
    let (io, store) = ringed(4);
    let name = key(11);
    let mut buf = Vec::new();
    {
        let mut read = pin!(Reads::get_async(&store, COLUMN, &name, &mut buf));
        let mut context = Context::from_waker(Waker::noop());
        assert!(read.as_mut().poll(&mut context).is_pending());
    }
    settle(&io);
    let leases: Vec<_> = (0..RING_SLOTS).map(|_slot| ring(&store).try_claim().expect("open").expect("a free slot")).collect();
    assert!(ring(&store).try_claim().expect("open").is_none());
    drop(leases);
    let seats: Vec<_> = (0..RING_READS).map(|_seat| ring(&store).try_seat().expect("open").expect("a free seat")).collect();
    drop(seats);
    assert!(run(&io, Reads::get_async(&store, COLUMN, &name, &mut buf)).expect("get"));
    assert_eq!(buf, value(11));
    store.close().expect("close");
}

// more reads than seats and slots wait in line, and each gets its own answer
#[test]
fn more_reads_than_seats() {
    let (io, store) = ringed(5);
    let names: Vec<Vec<u8>> = (0..MANY_READS).map(|number| key(number * 7)).collect();
    let mut bufs: Vec<Vec<u8>> = names.iter().map(|_name| Vec::new()).collect();
    let mut reads: Vec<Option<PendingRead<'_>>> = names
        .iter()
        .zip(bufs.iter_mut())
        .map(|(name, buf)| Some(Box::pin(Reads::get_async(&store, COLUMN, name, buf)) as PendingRead<'_>))
        .collect();
    let mut context = Context::from_waker(Waker::noop());
    for _step in 0..STEPS {
        for slot in &mut reads {
            if let Some(read) = slot
                && let Poll::Ready(found) = read.as_mut().poll(&mut context)
            {
                assert!(found.expect("get"));
                *slot = None;
            }
        }
        if reads.iter().all(Option::is_none) {
            break;
        }
        step(&io);
    }
    assert!(reads.iter().all(Option::is_none), "a read never finished");
    drop(reads);
    for (number, buf) in (0..MANY_READS).zip(&bufs) {
        assert_eq!(*buf, value(number * 7));
    }
    store.close().expect("close");
}

// a close lets a read in flight land, and a read after it hears the store has closed
#[test]
fn close_drains_the_ring() {
    let (io, store) = ringed(6);
    let name = key(13);
    let mut buf = Vec::new();
    let mut context = Context::from_waker(Waker::noop());
    {
        let mut read = pin!(Reads::get_async(&store, COLUMN, &name, &mut buf));
        assert!(read.as_mut().poll(&mut context).is_pending());
        store.close().expect("close");
        let answer = read.as_mut().poll(&mut context);
        assert!(answer.is_ready());
    }
    settle(&io);
    let late = run(&io, Reads::get_async(&store, COLUMN, &name, &mut buf));
    assert!(matches!(late, Err(StoreError::Closed)));
}

// a warm read through the ring takes nothing from the allocator
#[test]
fn no_allocations() {
    let (io, store) = ringed(7);
    let name = key(17);
    let mut buf = Vec::with_capacity(64);
    assert!(run(&io, Reads::get_async(&store, COLUMN, &name, &mut buf)).expect("get"));
    let (found, allocations, frees) = counted(|| run(&io, Reads::get_async(&store, COLUMN, &name, &mut buf)));
    assert!(found.expect("get"));
    assert_eq!((allocations, frees), (0, 0));
    store.close().expect("close");
}

// a batch answers in key order at one point, past a missing key
#[test]
fn batch_answers() {
    let (io, store) = ringed(8);
    let names: Vec<Vec<u8>> = (0..KEYS).step_by(13).map(key).collect();
    let mut keys: Vec<&[u8]> = names.iter().map(Vec::as_slice).collect();
    keys.push(b"missing");
    let mut values = ValueList::new();
    run(&io, Reads::get_many_async(&store, COLUMN, &keys, &mut values)).expect("many");
    assert_eq!(values.len(), keys.len());
    for (index, number) in (0..KEYS).step_by(13).enumerate() {
        let expected = value(number);
        assert_eq!(values.answer(index), Some(ValueAnswer::Found(&expected)));
    }
    assert_eq!(values.answer(keys.len() - 1), Some(ValueAnswer::Missing));
    store.close().expect("close");
}

// a scan streams every key through the ring, a block at a time
#[test]
fn scan_streams() {
    let (io, store) = ringed(9);
    let mut scan = store.iter_from(COLUMN, b"", Direction::Ascending).expect("scan");
    let (mut keys, mut seen) = (ValueList::new(), 0);
    loop {
        let has_more = run(&io, Scan::next_block(&mut scan, &mut keys)).expect("block");
        for index in 0..keys.len() {
            let expected = key(seen);
            assert_eq!(keys.answer(index), Some(ValueAnswer::Found(&expected)));
            seen += 1;
        }
        if !has_more {
            break;
        }
    }
    assert_eq!(seen, KEYS);
    drop(scan);
    store.close().expect("close");
}

// a scan dropped with a block in flight gives its buffers back, so every scan can still start
#[test]
fn dropped_block_ends_the_walk() {
    let (io, store) = ringed(10);
    let mut context = Context::from_waker(Waker::noop());
    let mut scan = store.iter_from(COLUMN, b"", Direction::Ascending).expect("scan");
    let mut keys = ValueList::new();
    {
        let mut block = pin!(Scan::next_block(&mut scan, &mut keys));
        assert!(block.as_mut().poll(&mut context).is_pending());
    }
    settle(&io);
    assert!(!run(&io, Scan::next_block(&mut scan, &mut keys)).expect("block"));
    drop(scan);
    let scans: Vec<_> = (0..MOST_SCANS).map(|_scan| store.iter_from(COLUMN, b"", Direction::Ascending).expect("scan")).collect();
    drop(scans);
    store.close().expect("close");
}

// a cue reads at its point through the ring
#[test]
fn cue_reads_its_point() {
    let (io, store) = ringed(11);
    let cue = store.cue().expect("cue");
    store.put(COLUMN, &key(3), b"newer").expect("put");
    let mut buf = Vec::new();
    assert!(run(&io, Reads::get_async(&cue, COLUMN, &key(3), &mut buf)).expect("get"));
    assert_eq!(buf, value(3));
    assert!(run(&io, Reads::get_async(&store, COLUMN, &key(3), &mut buf)).expect("get"));
    assert_eq!(buf, b"newer");
    drop(cue);
    store.close().expect("close");
}

struct WakeCount(AtomicUsize);

impl Wake for WakeCount {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::AcqRel);
    }
}

// a caller that left the line doesn't take the wake the next one needs
#[test]
fn wake_passes_a_caller_that_left() {
    let (_io, store) = ringed(31);
    let ring = ring(&store);
    let mut seats: Vec<_> = (0..RING_READS).map(|_seat| ring.try_seat().expect("open").expect("a free seat")).collect();
    {
        let mut left = pin!(ring.seat());
        let mut context = Context::from_waker(Waker::noop());
        assert!(left.as_mut().poll(&mut context).is_pending());
    }
    let wakes = Arc::new(WakeCount(AtomicUsize::new(0)));
    let waker = Waker::from(Arc::clone(&wakes));
    let mut context = Context::from_waker(&waker);
    let mut live = pin!(ring.seat());
    assert!(live.as_mut().poll(&mut context).is_pending());

    drop(seats.pop());
    assert_eq!(wakes.0.load(Ordering::Acquire), 1, "a seat is free and the caller in line sleeps");
}

// a wake that came to a caller who left passes to the next, so a dropped read never strands the line
#[test]
fn woken_caller_hands_on() {
    let (_io, store) = ringed(33);
    let ring = ring(&store);
    let mut seats: Vec<_> = (0..RING_READS).map(|_seat| ring.try_seat().expect("open").expect("a free seat")).collect();
    let wakes = Arc::new(WakeCount(AtomicUsize::new(0)));
    let waker = Waker::from(Arc::clone(&wakes));
    let mut context = Context::from_waker(&waker);
    {
        let mut first = pin!(ring.seat());
        assert!(first.as_mut().poll(&mut Context::from_waker(Waker::noop())).is_pending());
        let mut second = Box::pin(ring.seat());
        assert!(second.as_mut().poll(&mut context).is_pending());
        drop(seats.pop());
        assert_eq!(wakes.0.load(Ordering::Acquire), 0);
        let taken = ring.try_seat().expect("open");
        drop(taken);
    }
    drop(seats);
}

// a slot coming back wakes a caller waiting for a slot, with a caller waiting for a seat ahead of it in line
#[test]
fn slot_wake_reaches_a_slot_waiter() {
    let (_io, store) = ringed(32);
    let ring = ring(&store);
    let seats: Vec<_> = (0..RING_READS).map(|_seat| ring.try_seat().expect("open").expect("a free seat")).collect();
    let mut leases: Vec<_> = (0..RING_SLOTS).map(|_slot| ring.try_claim().expect("open").expect("a free slot")).collect();

    let seat_wakes = Arc::new(WakeCount(AtomicUsize::new(0)));
    let seat_waker = Waker::from(Arc::clone(&seat_wakes));
    let mut seat_context = Context::from_waker(&seat_waker);
    let mut wants_seat = pin!(ring.seat());
    assert!(wants_seat.as_mut().poll(&mut seat_context).is_pending());

    let slot_wakes = Arc::new(WakeCount(AtomicUsize::new(0)));
    let slot_waker = Waker::from(Arc::clone(&slot_wakes));
    let mut slot_context = Context::from_waker(&slot_waker);
    let mut wants_slot = pin!(ring.claim());
    assert!(wants_slot.as_mut().poll(&mut slot_context).is_pending());

    drop(leases.pop());
    // a runtime polls whoever the ring woke
    if seat_wakes.0.load(Ordering::Acquire) > 0 {
        assert!(wants_seat.as_mut().poll(&mut seat_context).is_pending());
    }
    assert_eq!(slot_wakes.0.load(Ordering::Acquire), 1, "a slot is free and the caller waiting for it sleeps");
    drop(seats);
}
