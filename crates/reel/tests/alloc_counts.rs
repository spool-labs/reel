//! Allocator calls per operation on each store door, counted on the calling thread
//! Run with `cargo test -p tape-reel --test alloc_counts -- --nocapture`

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use std::thread;

use tempfile::TempDir;

use reel::config::{ReelConfig, SyncPolicy, ThreadBudget};
use reel::format::column::{Codec, ColumnId, ColumnSet, ColumnSpec};
use reel::units::ByteCount;
use reel::{Direction, KeyWidth, ReelStore, Store, Value, WriteBatch};

thread_local! {
    /// How many allocator calls this thread has made since the last reading
    static CALLS: Cell<usize> = const { Cell::new(0) };

    /// Whether this thread is inside a counted stretch
    static COUNTING: Cell<bool> = const { Cell::new(false) };
}

/// The system allocator with a tally in front of it
struct Counting;

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        note();
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        note();
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        note();
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

/// Counts one allocator call while this thread is counting
fn note() {
    let _ = COUNTING.try_with(|on| {
        if on.get() {
            let _ = CALLS.try_with(|calls| calls.set(calls.get() + 1));
        }
    });
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// Counts the allocator calls `work` makes on this thread
fn counted<Out>(work: impl FnOnce() -> Out) -> (usize, Out) {
    CALLS.with(|calls| calls.set(0));
    COUNTING.with(|on| on.set(true));
    let out = work();
    COUNTING.with(|on| on.set(false));
    (CALLS.with(Cell::get), out)
}

/// Allocator calls per operation
fn per_op(calls: usize, ops: usize) -> f64 {
    calls as f64 / ops as f64
}

/// Unparks the thread that polls the future
struct Unparker(thread::Thread);

impl Wake for Unparker {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}

/// Drives one future to its answer without allocating
fn drive<Answered: Future>(future: Answered, context: &mut Context<'_>) -> Answered::Output {
    let mut future = std::pin::pin!(future);
    loop {
        if let Poll::Ready(answer) = future.as_mut().poll(context) {
            return answer;
        }
        thread::park();
    }
}

const ROWS: ColumnId = ColumnId(1);
const CF: &str = "rows";
const KEY_LEN: u16 = 16;

const COLUMNS: ColumnSet = &[ColumnSpec {
    id: ROWS,
    name: CF,
    key_width: KeyWidth::Fixed(KEY_LEN),
    shard_bytes: 0,
    purge_mark: None,
    codec: Codec::None,
}];

fn config() -> ReelConfig {
    ReelConfig {
        segment_bytes: ByteCount::mb(64),
        sync: SyncPolicy::Never,
        active_tails: ThreadBudget::threads(1),
        ..ReelConfig::default()
    }
}

fn open() -> (TempDir, ReelStore) {
    let home = TempDir::new().expect("tempdir");
    let root = PathBuf::from(home.path());
    let store = ReelStore::open(root, config(), COLUMNS).expect("open");
    (home, store)
}

fn key(at: u64) -> [u8; KEY_LEN as usize] {
    let mut bytes = [0u8; KEY_LEN as usize];
    bytes[..8].copy_from_slice(&at.to_be_bytes());
    bytes
}

/// Each reading covers this many records
const OPS: usize = 2_000;

/// A batched write makes fewer than this many allocations per record
const BATCHED_WRITE_CEILING: f64 = 0.5;

/// Payload bytes per record, large enough for the payload pool to keep its buffers
const PAYLOAD: usize = 1_200;

// what one operation of each door costs the allocator, printed as a table
#[test]
fn the_doors_are_priced() {
    let (_home, store) = open();
    let store: &dyn Store = &store;
    let payload = vec![0xABu8; PAYLOAD];

    // The first writes open a segment and warm the pools, so count a second run
    for at in 0..OPS as u64 {
        store.put(CF, &key(at), &payload).expect("put");
    }
    let (puts, ()) = counted(|| {
        for at in OPS as u64..2 * OPS as u64 {
            store.put(CF, &key(at), &payload).expect("put");
        }
    });

    let (gets, ()) = counted(|| {
        for at in 0..OPS as u64 {
            let found = store.get(CF, &key(at)).expect("get");
            assert!(found.is_some());
        }
    });

    let asked: Vec<[u8; KEY_LEN as usize]> = (0..64u64).map(key).collect();
    let borrowed: Vec<&[u8]> = asked.iter().map(|held| held.as_slice()).collect();
    let (many, ()) = counted(|| {
        for _ in 0..OPS / 64 {
            let found = store.get_many(CF, &borrowed).expect("many");
            assert_eq!(found.len(), 64);
        }
    });

    // Build the batches outside the reading, since the caller's buffers are the caller's cost
    let staged: Vec<WriteBatch> = (0..OPS as u64 / 64)
        .map(|round| {
            let mut batch = WriteBatch::new();
            for at in 0..64u64 {
                batch.put_owned(
                    CF,
                    key(1_000_000 + round * 64 + at).to_vec(),
                    payload.clone(),
                );
            }
            batch
        })
        .collect();
    let (batches, ()) = counted(|| {
        for batch in staged {
            store.write_batch(batch).expect("batch");
        }
    });

    let (walked, rows) = counted(|| {
        let mut rows = 0usize;
        for (_key, value) in store.iter(CF).expect("iter") {
            rows += value.len().min(1);
        }
        rows
    });

    let (lent, lent_rows) = counted(|| {
        let mut rows = 0usize;
        store
            .walk_from(CF, &key(0), Direction::Asc, 0, &mut |_key, value| {
                rows += value.len().min(1);
                true
            })
            .expect("walk");
        rows
    });

    // A prefix across the shard, so the counters cannot answer it and the walk steps every key
    let (keys_only, key_rows) = counted(|| store.count_prefix(CF, &[0u8]).expect("count"));

    // One allocation per put, the payload copy the tail takes ownership of
    assert!(
        per_op(puts, OPS) < 1.2,
        "a put costs {:.2} allocations against a floor of one",
        per_op(puts, OPS),
    );
    // A warm point read allocates nothing: the key is inline and the buffer comes off the pool
    assert!(
        per_op(gets, OPS) < 0.1,
        "a warm point read costs {:.2} allocations against a floor of none",
        per_op(gets, OPS),
    );
    // A batched write's payloads arrive owned, so it only allocates its lists, once per batch
    assert!(
        per_op(batches, OPS) < BATCHED_WRITE_CEILING,
        "a batched write costs {:.2} allocations against a ceiling of {BATCHED_WRITE_CEILING}",
        per_op(batches, OPS),
    );

    println!("path                allocs/op");
    println!("put                 {:>9.2}", per_op(puts, OPS));
    println!("get                 {:>9.2}", per_op(gets, OPS));
    println!("get_many (64)       {:>9.2}", per_op(many, OPS));
    println!("write_batch (64)    {:>9.2}", per_op(batches, OPS));
    println!("iter (owned)        {:>9.2}", per_op(walked, rows));
    println!("walk_from (lent)    {:>9.2}", per_op(lent, lent_rows));
    println!(
        "count_prefix        {:>9.2}",
        per_op(keys_only, key_rows as usize)
    );
}

// a wider batched read costs no more allocations than a narrow one
#[test]
fn a_batched_read_does_not_scale_with_its_width() {
    let (_home, store) = open();
    let store: &dyn Store = &store;
    let payload = vec![0xCDu8; PAYLOAD];
    for at in 0..512u64 {
        store.put(CF, &key(at), &payload).expect("put");
    }

    let mut readings = Vec::new();
    for width in [8usize, 64, 256] {
        let asked: Vec<[u8; KEY_LEN as usize]> = (0..width as u64).map(key).collect();
        let borrowed: Vec<&[u8]> = asked.iter().map(|held| held.as_slice()).collect();
        // Warm up so the first call's one-off work stays out of the reading
        let _ = store.get_many(CF, &borrowed).expect("many");
        let (calls, ()) = counted(|| {
            for _ in 0..32 {
                let found = store.get_many(CF, &borrowed).expect("many");
                assert_eq!(found.len(), width);
            }
        });
        readings.push((width, calls / 32));
    }

    for (width, calls) in &readings {
        println!("get_many of {width:>4} keys: {calls} allocs");
    }

    // Every list the submission uses belongs to the reading thread, so width adds no allocation
    let narrow = readings[0].1;
    for (width, calls) in &readings {
        assert!(
            *calls <= narrow,
            "a batch of {width} keys costs {calls} allocations where eight cost {narrow}",
        );
    }
}

// what the batched read door costs per submission beside the single door
#[test]
fn the_batch_door_has_a_fixed_price() {
    let (_home, store) = open();
    let store: &dyn Store = &store;
    let payload = vec![0x5Au8; PAYLOAD];
    for at in 0..256u64 {
        store.put(CF, &key(at), &payload).expect("put");
    }

    let mut readings = Vec::new();
    for width in [1usize, 8, 64] {
        let asked: Vec<[u8; KEY_LEN as usize]> = (0..width as u64).map(key).collect();
        let borrowed: Vec<&[u8]> = asked.iter().map(|held| held.as_slice()).collect();
        let _ = store.get_many(CF, &borrowed).expect("many");
        let (batched, ()) = counted(|| {
            for _ in 0..16 {
                let _ = store.get_many(CF, &borrowed).expect("many");
            }
        });
        let (looped, ()) = counted(|| {
            for _ in 0..16 {
                for held in &borrowed {
                    let _ = store.get(CF, held).expect("get");
                }
            }
        });
        println!(
            "{width:>3} keys: batched {:>3} allocs, looped {:>3}",
            batched / 16,
            looped / 16,
        );
        readings.push((width, batched / 16));
    }

    for (width, calls) in readings {
        assert!(
            calls < BATCH_CEILING,
            "a batch of {width} keys costs {calls} allocations against a ceiling of {BATCH_CEILING}",
        );
    }
}

/// A batched read makes fewer than this many allocations per submission, at any width
const BATCH_CEILING: usize = 8;

// a lent walk pays the batch price once per run
#[test]
fn a_lent_walk_pays_per_run_not_per_row() {
    let (_home, store) = open();
    let store: &dyn Store = &store;
    let payload = vec![0x77u8; PAYLOAD];
    for at in 0..4_096u64 {
        store.put(CF, &key(at), &payload).expect("put");
    }

    // Walk once first so the buffers, pool and read lists are warm
    store
        .walk_from(CF, &key(0), Direction::Asc, 0, &mut |_key, _value| true)
        .expect("walk");

    let mut readings = Vec::new();
    for rows in [8usize, 128, 1_024, 4_096] {
        let (calls, taken) = counted(|| {
            let mut taken = 0usize;
            store
                .walk_from(CF, &key(0), Direction::Asc, 0, &mut |_key, _value| {
                    taken += 1;
                    taken < rows
                })
                .expect("walk");
            taken
        });
        println!(
            "{rows:>5} rows lent: {calls:>5} allocs, {:.3}/row",
            per_op(calls, taken),
        );
        readings.push((rows, per_op(calls, taken)));
    }

    // Gate only the shortest and deepest walks and report the rows between
    for (rows, rate) in readings {
        let ceiling = match rows {
            8 => SHORT_WALK_CEILING,
            4_096 => LONG_WALK_CEILING,
            _ => continue,
        };
        assert!(
            rate < ceiling,
            "a lent walk of {rows} rows costs {rate:.3} allocations a row against a ceiling of {ceiling}",
        );
    }
}

/// An eight-row lent walk costs fewer than this many allocations per row
const SHORT_WALK_CEILING: f64 = 6.0;

/// A deep lent walk costs fewer than this many allocations per row
const LONG_WALK_CEILING: f64 = 0.12;

// an awaited read submits its one op without a vector to hold it
#[test]
fn an_awaited_read_carries_no_submission_vector() {
    let (_home, store) = open();
    let payload = vec![0x3Cu8; PAYLOAD];
    for at in 0..OPS as u64 {
        Store::put(&store, CF, &key(at), &payload).expect("put");
    }

    // Build the waker outside the reading, since it is the driver's cost
    let waker = Waker::from(Arc::new(Unparker(thread::current())));
    let mut context = Context::from_waker(&waker);

    // Warm up so the first read's one-off work stays out of the reading
    let _ = drive(Store::get_wait(&store, CF, &key(0)), &mut context).expect("wait");
    let (awaited, ()) = counted(|| {
        for at in 0..OPS as u64 {
            let found = drive(Store::get_wait(&store, CF, &key(at)), &mut context).expect("wait");
            assert!(found.is_some());
        }
    });

    println!("get_wait            {:>9.2}", per_op(awaited, OPS));
    assert!(
        per_op(awaited, OPS) < AWAITED_CEILING,
        "an awaited read costs {:.2} allocations against a ceiling of {AWAITED_CEILING}",
        per_op(awaited, OPS),
    );
}

/// An awaited read costs fewer than this many allocations
const AWAITED_CEILING: f64 = 0.4;

// a key walk that lends its buffer down steps without calling the allocator
#[test]
fn a_key_walk_lends_its_buffer() {
    let (_home, store) = open();
    let payload = vec![0x99u8; PAYLOAD];
    for at in 0..4_096u64 {
        Store::put(&store, CF, &key(at), &payload).expect("put");
    }

    let (calls, stepped) = counted(|| {
        let mut walk = store
            .iter_keys_from(CF, Some(&key(0)), Direction::Asc)
            .expect("keys");
        let mut held = Vec::new();
        let mut stepped = 0usize;
        while walk.next_into(&mut held) {
            stepped += 1;
        }
        stepped
    });

    println!(
        "{stepped} lent key steps: {calls} allocs, {:.3}/step",
        per_op(calls, stepped),
    );
    assert!(
        per_op(calls, stepped) < KEY_STEP_CEILING,
        "a lent key step costs {:.3} allocations against a ceiling of {KEY_STEP_CEILING}",
        per_op(calls, stepped),
    );
}

/// One step of a lending key walk costs fewer than this many allocations
const KEY_STEP_CEILING: f64 = 0.05;

// the key walk hands out an owned buffer per step and takes none back
#[test]
fn a_key_walk_allocates_per_step() {
    let (_home, store) = open();
    let payload = vec![0x99u8; PAYLOAD];
    for at in 0..4_096u64 {
        Store::put(&store, CF, &key(at), &payload).expect("put");
    }

    let (calls, stepped) = counted(|| {
        let mut walk = store
            .iter_keys_from(CF, Some(&key(0)), Direction::Asc)
            .expect("keys");
        let mut stepped = 0usize;
        while walk.next().is_some() {
            stepped += 1;
        }
        stepped
    });

    println!(
        "{stepped} key steps: {calls} allocs, {:.2}/step",
        per_op(calls, stepped)
    );
}

// what the value a read hands back costs when the caller takes it owned
#[test]
fn taking_a_value_owned_costs_one_more() {
    let (_home, store) = open();
    let store: &dyn Store = &store;
    let payload = vec![0xEFu8; PAYLOAD];
    for at in 0..256u64 {
        store.put(CF, &key(at), &payload).expect("put");
    }

    let (borrowed, ()) = counted(|| {
        for at in 0..256u64 {
            let found = store.get(CF, &key(at)).expect("get").expect("held");
            assert_eq!(found.len(), PAYLOAD);
        }
    });
    let (owned, ()) = counted(|| {
        for at in 0..256u64 {
            let found = store.get(CF, &key(at)).expect("get").expect("held");
            assert_eq!(Value::into_vec(found).len(), PAYLOAD);
        }
    });

    println!(
        "get, value read through:  {:.2} allocs/op",
        per_op(borrowed, 256)
    );
    println!(
        "get, value taken owned:   {:.2} allocs/op",
        per_op(owned, 256)
    );
}
