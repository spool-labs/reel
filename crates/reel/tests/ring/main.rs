//! The kernel's ring on real files: async reads answer as the blocking calls do, and hold no thread while they wait
#![cfg(target_os = "linux")]
#![allow(clippy::expect_used)]

use std::fs;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::{Pin, pin};
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use std::thread::{self, Thread};

use reel::config::{ColumnConfig, KeyShape, StoreConfig, SyncPolicy, VolumeConfig};
use reel::engine::reel_store::ReelStore;
use reel::io::plain::PlainIo;
use reel::record::codec::Codec;
use reel_api::column_id::ColumnId;
use reel_api::reads::{Reads, ValueWindow};
use reel_api::store_error::StoreError;
use reel_api::units::{ByteCount, VolumeId};
use reel_sim::harness_io::HarnessIo;

const COLUMN: ColumnId = ColumnId(1);
const BUDGET: ByteCount = ByteCount(256 * 1024 * 1024);
const KEYS: u64 = 20_000;
const READS_AT_ONCE: u64 = 200;

type Store = ReelStore<PlainIo>;
type PendingRead<'call> = Pin<Box<dyn Future<Output = Result<bool, StoreError>> + Send + 'call>>;

struct Unpark(Thread);

impl Wake for Unpark {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }
}

fn run<Answer>(future: impl Future<Output = Answer>) -> Answer {
    let waker = Waker::from(Arc::new(Unpark(thread::current())));
    let mut context = Context::from_waker(&waker);
    let mut future = pin!(future);
    loop {
        if let Poll::Ready(answer) = future.as_mut().poll(&mut context) {
            return answer;
        }
        thread::park();
    }
}

fn root(name: &str) -> PathBuf {
    let root = std::env::temp_dir().join(format!("reel-ring-{name}-{}", std::process::id()));
    let _gone = fs::remove_dir_all(&root);
    root
}

fn config(root: &Path) -> StoreConfig {
    StoreConfig {
        memory_budget: BUDGET,
        threads: Default::default(),
        volumes: vec![VolumeConfig {
            name: "ring".to_string(),
            roots: vec![root.to_path_buf()],
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
            least_value: None,
        }],
    }
}

fn key(number: u64) -> Vec<u8> {
    format!("key-{number:06}").into_bytes()
}

fn value(number: u64) -> Vec<u8> {
    format!("value-{number:06}-{}", "x".repeat((number % 700) as usize)).into_bytes()
}

// a store with its keys sealed and nothing cached, so reads reach the device
fn filled(root: &Path) -> Store {
    let store = ReelStore::open(PlainIo::new(), &config(root)).expect("open");
    for number in 0..KEYS {
        store.put(COLUMN, &key(number), &value(number)).expect("put");
    }
    store.close().expect("close");
    drop(store);
    ReelStore::open(PlainIo::new(), &config(root)).expect("open")
}

// this kernel gives the store a ring, so every test here reads through it
#[test]
fn store_has_a_ring() {
    let root = root("has");
    let store = filled(&root);
    assert!(store.state.ring.is_some());
    store.close().expect("close");
    fs::remove_dir_all(&root).expect("remove");
}

// each form answers as the blocking call does
#[test]
fn forms_match_blocking() {
    let root = root("forms");
    let store = filled(&root);
    let (mut buf, mut blocking) = (Vec::new(), Vec::new());
    for number in (0..KEYS).step_by(37) {
        assert!(run(Reads::get_async(&store, COLUMN, &key(number), &mut buf)).expect("get"));
        assert!(store.get(COLUMN, &key(number), &mut blocking).expect("get"));
        assert_eq!(buf, blocking);
        assert_eq!(buf, value(number));
    }
    assert!(!run(Reads::get_async(&store, COLUMN, b"missing", &mut buf)).expect("get"));
    let window = ValueWindow { offset: ByteCount(6), length: ByteCount(6) };
    assert!(run(Reads::get_range_async(&store, COLUMN, &key(1_234), window, &mut buf)).expect("window"));
    assert_eq!(buf, b"001234");
    store.close().expect("close");
    fs::remove_dir_all(&root).expect("remove");
}

// reads at once past every slot and seat each get their own answer
#[test]
fn many_at_once() {
    let root = root("many");
    let store = filled(&root);
    let names: Vec<Vec<u8>> = (0..READS_AT_ONCE).map(|number| key(number * 83)).collect();
    let mut bufs: Vec<Vec<u8>> = names.iter().map(|_name| Vec::new()).collect();
    let mut reads: Vec<Option<PendingRead<'_>>> = names
        .iter()
        .zip(bufs.iter_mut())
        .map(|(name, buf)| Some(Box::pin(Reads::get_async(&store, COLUMN, name, buf)) as PendingRead<'_>))
        .collect();
    let waker = Waker::from(Arc::new(Unpark(thread::current())));
    let mut context = Context::from_waker(&waker);
    while reads.iter().any(Option::is_some) {
        for slot in &mut reads {
            if let Some(read) = slot
                && let Poll::Ready(found) = read.as_mut().poll(&mut context)
            {
                assert!(found.expect("get"));
                *slot = None;
            }
        }
        thread::yield_now();
    }
    drop(reads);
    for (number, buf) in (0..READS_AT_ONCE).zip(&bufs) {
        assert_eq!(*buf, value(number * 83));
    }
    store.close().expect("close");
    fs::remove_dir_all(&root).expect("remove");
}

// a store opened without a ring answers the same, on the blocking pool
#[test]
fn pool_still_answers() {
    let root = root("pool");
    filled(&root).close().expect("close");
    let store = ReelStore::open(HarnessIo::ringless(), &config(&root)).expect("open");
    assert!(store.state.ring.is_none());
    let mut buf = Vec::new();
    assert!(run(Reads::get_async(&store, COLUMN, &key(5), &mut buf)).expect("get"));
    assert_eq!(buf, value(5));
    store.close().expect("close");
    fs::remove_dir_all(&root).expect("remove");
}

// bytes the page cache holds answer on the first poll on a real file, on the ring and on the pool
#[test]
fn warm_get_answers_first_poll() {
    // tmpfs has no read that refuses to wait, and the temp directory is often one
    let root = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!("reel-warm-{}", std::process::id()));
    let _gone = fs::remove_dir_all(&root);
    filled(&root).close().expect("close");
    for io in [HarnessIo::ringed(), HarnessIo::ringless()] {
        let store = ReelStore::open(io, &config(&root)).expect("open");
        let (name, mut buf) = (key(7), Vec::new());
        assert!(store.get(COLUMN, &name, &mut buf).expect("get"));
        buf.clear();
        {
            let mut read = pin!(Reads::get_async(&store, COLUMN, &name, &mut buf));
            let mut context = Context::from_waker(Waker::noop());
            assert!(matches!(read.as_mut().poll(&mut context), Poll::Ready(Ok(true))));
        }
        assert_eq!(buf, value(7));
        store.close().expect("close");
    }
    fs::remove_dir_all(&root).expect("remove");
}
