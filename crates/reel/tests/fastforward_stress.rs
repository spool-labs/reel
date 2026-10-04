//! FastForward point reads under writers, deletes, seals, handovers and compaction, drawn from one seed
//!
//! Segments are tiny and the volume pages, so seals hand keys to FastForward, compaction
//! repoints and retires segments, and the cleaner runs, all while readers run. Each key
//! has one writer, which marks an op started before it runs and completed after. A read
//! must answer a put whose op falls between the op completed when it began and the op
//! started when it ended, and may answer nothing only when a delete falls there too.
//! Once writers stop, and again after a reopen, every key answers its last op.
//!
//! Off Linux the page-cache probe always answers cold, so an overwrite always reads the
//! displaced header from the device. On Linux the same run takes the cached path too.
//!
//! Knobs: REEL_FF_SEEDS (how many seeds, default 4), REEL_FF_FIRST (the first seed, default
//! 1), REEL_FF_OPS (ops per writer, default 2000) and REEL_FF_SEED (one seed to replay).

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;

use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use tempfile::TempDir;

use reel::{
    ByteCount, Codec, ColumnId, ColumnSet, ColumnSpec, IndexResidency, KeyWidth, MapShape,
    ReelConfig, ReelStore, SyncPolicy, ThreadBudget,
};
use reel_core::Store;

const COLUMNS: ColumnSet = &[ColumnSpec {
    id: ColumnId(1),
    name: "rows",
    key_width: KeyWidth::Fixed(8),
    shard_bytes: 0,
    purge_mark: None,
    codec: Codec::None,
    map_shape: MapShape::Tree,
}];

const KEYS: u64 = 1200;

/// Share of ops that delete, in hundredths
const DELETE_SHARE: u32 = 25;

fn knob(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

/// A value that carries its key and op and fills the rest from both, so a torn or misplaced read cannot pass
fn value_of(key: u64, op: u64, len: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(16 + len);
    out.extend_from_slice(&key.to_be_bytes());
    out.extend_from_slice(&op.to_be_bytes());
    let mut state = key.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ op;
    for _ in 0..len {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        out.push(state as u8);
    }
    out
}

/// The op a value claims, once its key and filler check out
fn op_of(seed: u64, key: u64, value: &[u8]) -> u64 {
    assert!(value.len() >= 16, "seed {seed}: key {key} value of {} bytes", value.len());
    let named = u64::from_be_bytes(value[..8].try_into().expect("key bytes"));
    let op = u64::from_be_bytes(value[8..16].try_into().expect("op bytes"));
    assert_eq!(named, key, "seed {seed}: key {key} served the value of key {named}");
    assert_eq!(
        value,
        value_of(key, op, value.len() - 16).as_slice(),
        "seed {seed}: key {key} op {op} came back torn"
    );
    op
}

fn config(rng: &mut SmallRng) -> ReelConfig {
    ReelConfig {
        segment_bytes: ByteCount::from_bytes(64 * 1024),
        alloc_chunk: ByteCount::from_bytes(16 * 1024),
        sync: SyncPolicy::Never,
        active_tails: ThreadBudget::threads(rng.gen_range(1..=4)),
        compact_dead_ratio: 0.1,
        scrub_mbps: 0,
        index: IndexResidency::Paged,
        ..ReelConfig::default()
    }
}

/// What each key's one writer has done, op by op
struct Ledger {
    /// The newest op a writer has begun on each key
    started: Vec<AtomicU64>,

    /// The newest op a writer has finished on each key
    completed: Vec<AtomicU64>,

    /// The ops that deleted each key
    deletes: Vec<Mutex<BTreeSet<u64>>>,
}

impl Ledger {
    fn new() -> Ledger {
        Ledger {
            started: (0..KEYS).map(|_| AtomicU64::new(0)).collect(),
            completed: (0..KEYS).map(|_| AtomicU64::new(0)).collect(),
            deletes: (0..KEYS).map(|_| Mutex::new(BTreeSet::new())).collect(),
        }
    }

    fn is_delete(&self, key: u64, op: u64) -> bool {
        self.deletes[key as usize].lock().expect("deletes").contains(&op)
    }

    /// Whether a read that began after `floor` completed and ended before `ceiling` started may answer this
    fn admits(&self, key: u64, floor: u64, ceiling: u64, answer: Option<u64>) -> bool {
        match answer {
            Some(op) => op >= floor && op <= ceiling && !self.is_delete(key, op),
            None => floor == 0 || self.deletes[key as usize].lock().expect("deletes").range(floor..=ceiling).next().is_some(),
        }
    }
}

fn writer(store: &ReelStore, ledger: &Ledger, seed: u64, id: u64, writers: u64, ops: u64) {
    let mut rng = SmallRng::seed_from_u64(seed ^ (id << 40) ^ 0xA5A5);
    let owned: Vec<u64> = (0..KEYS).filter(|key| key % writers == id).collect();
    for _ in 0..ops {
        let key = owned[rng.gen_range(0..owned.len())];
        let op = ledger.started[key as usize].load(Ordering::Acquire) + 1;
        let is_delete = rng.gen_range(0..100u32) < DELETE_SHARE;
        if is_delete {
            ledger.deletes[key as usize].lock().expect("deletes").insert(op);
        }
        ledger.started[key as usize].store(op, Ordering::Release);
        match is_delete {
            true => Store::delete(store, "rows", &key.to_be_bytes()).expect("delete"),
            false => {
                let len = rng.gen_range(0..400);
                Store::put(store, "rows", &key.to_be_bytes(), &value_of(key, op, len)).expect("put");
            }
        }
        ledger.completed[key as usize].store(op, Ordering::Release);
    }
}

fn reader(store: &ReelStore, ledger: &Ledger, seed: u64, id: u64, done: &AtomicBool) -> u64 {
    let mut rng = SmallRng::seed_from_u64(seed ^ (id << 48) ^ 0x5A5A);
    let mut read = 0u64;
    while !done.load(Ordering::Relaxed) {
        let key = rng.gen_range(0..KEYS);
        let floor = ledger.completed[key as usize].load(Ordering::Acquire);
        let got = Store::get(store, "rows", &key.to_be_bytes()).expect("get");
        let ceiling = ledger.started[key as usize].load(Ordering::Acquire);
        let answer = got.map(|value| op_of(seed, key, &value));
        assert!(
            ledger.admits(key, floor, ceiling, answer),
            "seed {seed}: key {key} answered {answer:?} with op {floor} done before the read and {ceiling} begun after"
        );
        read += 1;
    }
    read
}

/// Every key answers its last op, once nothing writes
fn settled(store: &ReelStore, ledger: &Ledger, seed: u64, stage: &str) {
    for key in 0..KEYS {
        let last = ledger.completed[key as usize].load(Ordering::Acquire);
        let want = (last != 0 && !ledger.is_delete(key, last)).then_some(last);
        let got = Store::get(store, "rows", &key.to_be_bytes()).expect("get");
        let answer = got.map(|value| op_of(seed, key, &value));
        assert_eq!(answer, want, "seed {seed}: key {key} after {stage}");
    }
}

fn run(seed: u64) {
    let mut rng = SmallRng::seed_from_u64(seed);
    let dir = TempDir::new().expect("tempdir");
    let config = config(&mut rng);
    let store = Arc::new(ReelStore::open(dir.path().to_path_buf(), config.clone(), COLUMNS).expect("open"));
    let ledger = Arc::new(Ledger::new());
    let ops = knob("REEL_FF_OPS", 2000);
    let writers = rng.gen_range(1..=3u64);
    let readers = rng.gen_range(2..=4u64);
    let done = Arc::new(AtomicBool::new(false));

    let read = thread::scope(|scope| {
        let maintainer = {
            let (store, done) = (store.clone(), done.clone());
            scope.spawn(move || {
                while !done.load(Ordering::Relaxed) {
                    Store::maintain(&*store).expect("maintain");
                    thread::yield_now();
                }
            })
        };
        let reading: Vec<_> = (0..readers)
            .map(|id| {
                let (store, ledger, done) = (store.clone(), ledger.clone(), done.clone());
                scope.spawn(move || reader(&store, &ledger, seed, id, &done))
            })
            .collect();
        let writing: Vec<_> = (0..writers)
            .map(|id| {
                let (store, ledger) = (store.clone(), ledger.clone());
                scope.spawn(move || writer(&store, &ledger, seed, id, writers, ops))
            })
            .collect();
        for writing in writing {
            writing.join().expect("writer");
        }
        done.store(true, Ordering::Relaxed);
        maintainer.join().expect("maintainer");
        reading.into_iter().map(|reading| reading.join().expect("reader")).sum::<u64>()
    });
    assert!(read > 0, "seed {seed}: readers took nothing");
    assert!(store.index().fast_held() > 0, "seed {seed}: nothing reached FastForward");
    let compaction = store.compaction_counters();
    println!(
        "seed {seed}: {writers} writers, {readers} readers, {read} reads, FastForward holds {}, {} segments rewritten, {} unlinked whole",
        store.index().fast_held(),
        compaction.segments_rewritten,
        compaction.segments_unlinked_whole,
    );

    Store::maintain(&*store).expect("maintain");
    settled(&store, &ledger, seed, "the writers stopped");

    let store = Arc::into_inner(store).expect("one owner");
    store.close().expect("close");
    drop(store);
    let reopened = ReelStore::open(dir.path().to_path_buf(), config, COLUMNS).expect("reopen");
    settled(&reopened, &ledger, seed, "a reopen");
}

#[test]
fn fastforward_reads_hold_under_writes_and_compaction() {
    if let Some(seed) = std::env::var("REEL_FF_SEED").ok().and_then(|seed| seed.parse().ok()) {
        run(seed);
        return;
    }
    let first = knob("REEL_FF_FIRST", 1);
    for seed in first..first + knob("REEL_FF_SEEDS", 4) {
        run(seed);
    }
}
