//! Async reads: every form answers as its blocking twin does, through fixed slots and a bounded pool of workers

use std::future::Future;
use std::path::PathBuf;
use std::pin::{Pin, pin};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, Wake, Waker};

use reel::config::{ColumnConfig, ColumnIndex, KeyShape, StoreConfig, SyncPolicy, VolumeConfig};
use reel::engine::read_pool::{SCAN_BLOCK_KEYS, SLOT_KEYS};
use reel::engine::reel_store::ReelStore;
use reel::record::codec::Codec;
use reel_core::column_id::ColumnId;
use reel_core::direction::Direction;
use reel_core::reads::{Reads, ValueWindow};
use reel_core::scan::{KeyScan, Scan};
use reel_core::store::Store;
use reel_core::store_error::StoreError;
use reel_core::units::{ByteCount, Nanoseconds, VolumeId};
use reel_core::value_list::{ValueAnswer, ValueList};
use reel_sim::seeded_random::Seed;
use reel_sim::simulated_io::SimulatedIo;

use crate::counting::counted;

const ROOT: &str = "/disk/async";
const COLUMN: ColumnId = ColumnId(1);
const BUDGET: ByteCount = ByteCount(64 * 1024 * 1024);
const TICK: Nanoseconds = Nanoseconds(1_000_000);
const STEPS: u64 = 400_000;
const KEYS: u64 = 3_000;
const WARM_ROUNDS: u64 = 4;

type Store2 = ReelStore<SimulatedIo>;
type PendingRead<'call> = Pin<Box<dyn Future<Output = Result<bool, StoreError>> + Send + 'call>>;

fn open(io: &SimulatedIo) -> Store2 {
    let config = StoreConfig {
        memory_budget: BUDGET,
        threads: reel::config::NO_WORKERS,
        volumes: vec![VolumeConfig {
            name: "async".to_string(),
            roots: vec![PathBuf::from(ROOT)],
            sync: SyncPolicy::Never,
            device: Default::default(), open_segments: None, segment_bytes: None, lifetime_groups: None,
        }],
        columns: vec![ColumnConfig {
            id: COLUMN,
            name: "column".to_string(),
            key_shape: KeyShape::Variable { most: 64 },
            codec: Codec::None,
            volume: VolumeId(0),
            index: ColumnIndex::Plain,
            least_rows: None,
            least_value: None,
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

// the simulator's jobs run between polls, so a read completes on this thread
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

fn filled(io: &SimulatedIo) -> Store2 {
    let store = open(io);
    for number in 0..KEYS {
        store.put(COLUMN, &key(number), &value(number)).expect("put");
    }
    store.seal_now();
    settle(io);
    store
}

// a store passes as the traits a consumer codes against
fn read_through<Backend: Store>(store: &Backend, name: &[u8], buf: &mut Vec<u8>) -> Result<bool, StoreError> {
    Reads::get(store, COLUMN, name, buf)
}

// a get, a window and a batch answer as the blocking calls do, sealed or open, and through the traits
#[test]
fn every_form_answers() {
    let io = SimulatedIo::new(Seed(1));
    let store = filled(&io);
    store.put(COLUMN, b"open", b"open value").expect("put");

    let mut buf = Vec::new();
    assert!(run(&io, Reads::get_async(&store, COLUMN, &key(7), &mut buf)).expect("get"));
    assert_eq!(buf, value(7));
    assert!(run(&io, Reads::get_async(&store, COLUMN, b"open", &mut buf)).expect("get"));
    assert_eq!(buf, b"open value");
    assert!(!run(&io, Reads::get_async(&store, COLUMN, b"missing", &mut buf)).expect("get"));

    let window = ValueWindow { offset: ByteCount(6), length: ByteCount(5) };
    assert!(run(&io, Reads::get_range_async(&store, COLUMN, &key(7), window, &mut buf)).expect("window"));
    assert_eq!(buf, b"00007");

    assert!(read_through(&store, &key(9), &mut buf).expect("get"));
    assert_eq!(buf, value(9));
    store.close().expect("close");
}

// a batch longer than a slot goes through in turns, at one point, in key order
#[test]
fn long_batch_in_turns() {
    let io = SimulatedIo::new(Seed(2));
    let store = filled(&io);
    let names: Vec<Vec<u8>> = (0..SLOT_KEYS as u64 * 2 + 5).map(|number| key(number % KEYS)).collect();
    let keys: Vec<&[u8]> = names.iter().map(Vec::as_slice).collect();
    let mut values = ValueList::new();
    run(&io, Reads::get_many_async(&store, COLUMN, &keys, &mut values)).expect("many");
    assert_eq!(values.len(), keys.len());
    for index in 0..keys.len() {
        let expected = value(index as u64 % KEYS);
        assert_eq!(values.answer(index), Some(ValueAnswer::Found(&expected)));
    }
    store.close().expect("close");
}

const OPEN_KEYS: u64 = 400;
const ASK_EVERY: usize = 7;

// get_many_async reads open records with the sealed ones, and answers as get does over misses, a delete and keys asked twice
#[test]
fn many_async_over_open_and_sealed() {
    let io = SimulatedIo::new(Seed(19));
    let store = filled(&io);
    for number in KEYS..KEYS + OPEN_KEYS {
        store.put(COLUMN, &key(number), &value(number)).expect("put");
    }
    store.delete(COLUMN, &key(KEYS + 1)).expect("delete");
    let mut names: Vec<Vec<u8>> = (0..KEYS + OPEN_KEYS).step_by(ASK_EVERY).map(key).collect();
    names.extend([b"missing".to_vec(), key(KEYS + 1), key(3), key(KEYS + 2), key(3)]);
    let keys: Vec<&[u8]> = names.iter().map(Vec::as_slice).collect();
    let mut values = ValueList::new();
    run(&io, Reads::get_many_async(&store, COLUMN, &keys, &mut values)).expect("many");
    assert_eq!(values.len(), keys.len());
    let mut buf = Vec::new();
    for (index, name) in names.iter().enumerate() {
        let is_found = run(&io, Reads::get_async(&store, COLUMN, name, &mut buf)).expect("get");
        let expected = if is_found { ValueAnswer::Found(&buf) } else { ValueAnswer::Missing };
        assert_eq!(values.answer(index), Some(expected), "key {name:?}");
    }
    store.close().expect("close");
}

// a cue's async reads see its point
#[test]
fn cue_forms() {
    let io = SimulatedIo::new(Seed(3));
    let store = filled(&io);
    let cue = store.cue().expect("cue");
    store.put(COLUMN, &key(1), b"newer").expect("put");
    store.delete(COLUMN, &key(2)).expect("delete");

    let mut buf = Vec::new();
    assert!(run(&io, Reads::get_async(&cue, COLUMN, &key(1), &mut buf)).expect("get"));
    assert_eq!(buf, value(1));
    let (first, second) = (key(1), key(2));
    let keys: [&[u8]; 2] = [&first, &second];
    let mut values = ValueList::new();
    run(&io, Reads::get_many_async(&cue, COLUMN, &keys, &mut values)).expect("many");
    let expected = value(2);
    assert_eq!(values.answer(1), Some(ValueAnswer::Found(&expected)));
    drop(cue);
    store.close().expect("close");
}

// a walk streams a block of keys at a time to its end, in order, and can go on by steps between blocks
#[test]
fn scan_streams() {
    let io = SimulatedIo::new(Seed(4));
    let store = filled(&io);
    let mut scan = store.iter_from(COLUMN, b"", Direction::Ascending).expect("scan");
    let (mut keys, mut name, mut seen) = (ValueList::new(), Vec::new(), 0);
    loop {
        let has_more = run(&io, Scan::next_block(&mut scan, &mut keys)).expect("block");
        assert!(keys.len() <= SCAN_BLOCK_KEYS);
        for index in 0..keys.len() {
            let expected = key(seen);
            assert_eq!(keys.answer(index), Some(ValueAnswer::Found(&expected)));
            seen += 1;
        }
        if !has_more {
            break;
        }
        assert_eq!(KeyScan::next_key(&mut scan, &mut name).expect("step"), reel_core::scan::ScanStep::Found);
        assert_eq!(name, key(seen));
        seen += 1;
    }
    assert_eq!(seen, KEYS);
    drop(scan);
    store.close().expect("close");
}

// more reads than slots wait in line, and each gets its answer
#[test]
fn line_for_a_slot() {
    let io = SimulatedIo::new(Seed(5));
    let store = filled(&io);
    let count = store.state.read_pool.slot_count() as u64 * 3;
    let names: Vec<Vec<u8>> = (0..count).map(key).collect();
    let mut buffers: Vec<Vec<u8>> = (0..count).map(|_buffer| Vec::new()).collect();
    let mut reads: Vec<PendingRead<'_>> = Vec::new();
    for (name, buf) in names.iter().zip(buffers.iter_mut()) {
        reads.push(Box::pin(Reads::get_async(&store, COLUMN, name, buf)));
    }
    let mut context = Context::from_waker(Waker::noop());
    for _step in 0..STEPS {
        reads.retain_mut(|read| match read.as_mut().poll(&mut context) {
            Poll::Ready(answer) => {
                assert!(answer.expect("get"));
                false
            },
            Poll::Pending => true,
        });
        if reads.is_empty() {
            break;
        }
        step(&io);
    }
    assert!(reads.is_empty());
    drop(reads);
    for (number, buf) in buffers.iter().enumerate() {
        assert_eq!(*buf, value(number as u64));
    }
    store.close().expect("close");
}

// a read dropped part way gives its slot back, and a walk dropped part way its buffers
#[test]
fn dropped_part_way() {
    let io = SimulatedIo::new(Seed(6));
    let store = filled(&io);
    let name = key(1);
    let mut context = Context::from_waker(Waker::noop());
    for _round in 0..store.state.read_pool.slot_count() * 2 {
        let mut buf = Vec::new();
        let mut read = Box::pin(Reads::get_async(&store, COLUMN, &name, &mut buf));
        assert!(read.as_mut().poll(&mut context).is_pending());
        drop(read);
        settle(&io);
    }
    for _round in 0..store.state.read_pool.scan_count * 2 {
        let mut scan = store.iter_from(COLUMN, b"", Direction::Ascending).expect("scan");
        let mut keys = ValueList::new();
        let mut block = Box::pin(Scan::next_block(&mut scan, &mut keys));
        assert!(block.as_mut().poll(&mut context).is_pending());
        drop(block);
        drop(scan);
        settle(&io);
    }
    let mut buf = Vec::new();
    assert!(run(&io, Reads::get_async(&store, COLUMN, &name, &mut buf)).expect("get"));
    assert!(store.iter_from(COLUMN, b"", Direction::Ascending).is_ok());
    store.close().expect("close");
}

// a store that closes under a queued read ends it with the error
#[test]
fn closed_under_a_read() {
    let io = SimulatedIo::new(Seed(7));
    let store = filled(&io);
    let (name, mut buf) = (key(1), Vec::new());
    let mut context = Context::from_waker(Waker::noop());
    let mut read = Box::pin(Reads::get_async(&store, COLUMN, &name, &mut buf));
    assert!(read.as_mut().poll(&mut context).is_pending());
    store.close().expect("close");
    assert!(matches!(read.as_mut().poll(&mut context), Poll::Ready(Err(StoreError::Closed))));
}

// a warm async read allocates nothing, job included
#[test]
fn no_allocations() {
    let io = SimulatedIo::new(Seed(8));
    let store = filled(&io);
    let name = key(1);
    let (first, second) = (key(1), key(2));
    let keys: [&[u8]; 2] = [&first, &second];
    let (mut buf, mut values, mut block) = (Vec::with_capacity(256), ValueList::new(), ValueList::new());
    let round = |buf: &mut Vec<u8>, values: &mut ValueList, block: &mut ValueList| {
        run(&io, Reads::get_async(&store, COLUMN, &name, buf)).expect("get");
        run(&io, Reads::get_many_async(&store, COLUMN, &keys, values)).expect("many");
        let mut scan = store.iter_from(COLUMN, b"", Direction::Ascending).expect("scan");
        run(&io, Scan::next_block(&mut scan, block)).expect("block");
    };
    for _round in 0..WARM_ROUNDS {
        round(&mut buf, &mut values, &mut block);
    }
    let (_answer, allocations, frees) = counted(|| round(&mut buf, &mut values, &mut block));
    assert_eq!((allocations, frees), (0, 0));
    store.close().expect("close");
}

/// A waker that remembers it was woken
#[derive(Default)]
struct Flag(AtomicBool);

impl Wake for Flag {
    fn wake(self: Arc<Self>) {
        self.0.store(true, Ordering::Release);
    }
}

// a caller polled once hears of its slot and of its answer, so a runtime never has to poll it again to find out
#[test]
fn wakes_its_caller() {
    let io = SimulatedIo::new(Seed(9));
    let store = filled(&io);
    let name = key(1);
    let mut buffers: Vec<Vec<u8>> = (0..=store.state.read_pool.slot_count()).map(|_buffer| Vec::new()).collect();
    let (last, others) = buffers.split_last_mut().expect("buffers");
    let mut context = Context::from_waker(Waker::noop());
    let mut reads: Vec<PendingRead<'_>> = Vec::new();
    for buf in others {
        let mut read: PendingRead<'_> = Box::pin(Reads::get_async(&store, COLUMN, &name, buf));
        assert!(read.as_mut().poll(&mut context).is_pending());
        reads.push(read);
    }

    let flag = Arc::new(Flag::default());
    let waker = Waker::from(Arc::clone(&flag));
    let mut flagged = Context::from_waker(&waker);
    let mut waiting = Box::pin(Reads::get_async(&store, COLUMN, &name, last));
    assert!(waiting.as_mut().poll(&mut flagged).is_pending());
    settle(&io);
    assert!(!flag.0.load(Ordering::Acquire));

    // the workers have answered, and a slot goes free when its caller takes the answer
    assert!(matches!(reads[0].as_mut().poll(&mut context), Poll::Ready(Ok(true))));
    drop(reads.remove(0));
    assert!(flag.0.swap(false, Ordering::AcqRel));
    assert!(waiting.as_mut().poll(&mut flagged).is_pending());
    settle(&io);
    assert!(flag.0.load(Ordering::Acquire));
    assert!(matches!(waiting.as_mut().poll(&mut flagged), Poll::Ready(Ok(true))));
    drop(waiting);
    drop(reads);
    store.close().expect("close");
}
