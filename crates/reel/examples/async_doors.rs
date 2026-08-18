//! The awaited door beside the blocking one
//!
//! Every read and write has both forms, they land the same records, and a batch
//! is one durability point either way. The engine is runtime agnostic: any
//! executor drives these futures, and the park and unpark one below is here to
//! prove that none is required rather than to be reached for.
//!
//! cargo run --example async_doors

use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use std::thread::{self, Thread};

use reel::{
    ByteCount, Codec, ColumnId, ColumnSet, ColumnSpec, KeyWidth, MapShape, ReelConfig, ReelStore,
    Store, StoreResult, Value, WriteBatch,
};

const RECORDS: &str = "records";

/// Eight byte keys, sharded on their leading byte, values stored raw
const COLUMNS: ColumnSet = &[ColumnSpec {
    id: ColumnId(1),
    name: RECORDS,
    key_width: KeyWidth::Fixed(8),
    shard_bytes: 1,
    inline_max: 0,
    row_carry: 0,
    purge_mark: None,
    codec: Codec::None,
    map_shape: MapShape::Tree,
}];

/// The whole runtime: a wake unparks the thread that is polling
struct Unparker(Thread);

impl Wake for Unparker {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}

/// Drive one future to its answer on the calling thread
fn block_on<Answered: Future>(future: Answered) -> Answered::Output {
    let mut future = Box::pin(future);
    let waker = Waker::from(Arc::new(Unparker(thread::current())));
    let mut context = Context::from_waker(&waker);
    loop {
        if let Poll::Ready(answer) = future.as_mut().poll(&mut context) {
            return answer;
        }
        thread::park();
    }
}

fn key(byte: u8) -> [u8; 8] {
    [byte; 8]
}

fn main() -> StoreResult<()> {
    let dir = tempfile::tempdir()?;
    let config = ReelConfig {
        segment_bytes: ByteCount::mb(16),
        alloc_chunk: ByteCount::mb(1),
        ..ReelConfig::default()
    };
    let store = ReelStore::open(dir.path().to_path_buf(), config, COLUMNS)?;

    // Through the trait rather than the engine's own methods of the same names, which
    // take a resolved key. The awaited door is not object safe, since the futures are
    // the backend's own types.
    Store::put(&store, RECORDS, &key(1), b"landed on this thread")?;
    block_on(Store::put_wait(
        &store,
        RECORDS,
        &key(2),
        b"landed from a future",
    ))?;

    let blocking = Store::get(&store, RECORDS, &key(2))?.map(Value::into_vec);
    let awaited = block_on(Store::get_wait(&store, RECORDS, &key(1)))?.map(Value::into_vec);
    assert_eq!(blocking, Some(b"landed from a future".to_vec()));
    assert_eq!(awaited, Some(b"landed on this thread".to_vec()));
    println!("put and get: each door reads back what the other wrote");

    let mut blocking_batch = WriteBatch::new();
    blocking_batch.put(RECORDS, &key(3), b"batched");
    blocking_batch.delete(RECORDS, &key(1));
    Store::write_batch(&store, blocking_batch)?;

    let mut awaited_batch = WriteBatch::new();
    awaited_batch.put(RECORDS, &key(4), b"batched");
    awaited_batch.delete(RECORDS, &key(2));
    block_on(Store::write_batch_wait(&store, awaited_batch))?;

    assert!(Store::get(&store, RECORDS, &key(1))?.is_none());
    assert!(Store::get(&store, RECORDS, &key(2))?.is_none());
    for written in [key(3), key(4)] {
        let found = block_on(Store::get_wait(&store, RECORDS, &written))?;
        assert_eq!(found.map(Value::into_vec), Some(b"batched".to_vec()));
    }
    println!("batches: a put and a tombstone landed together through both doors");

    // The awaited flush runs its fsync where blocking is allowed rather than on the
    // caller's worker, and both doors pay for it in device flushes.
    let before = store.sync_count();
    store.flush()?;
    let after_blocking = store.sync_count();
    Store::put(&store, RECORDS, &key(5), b"settled but not synced")?;
    block_on(store.flush_wait())?;
    let after_awaited = store.sync_count();

    assert!(
        after_blocking > before,
        "settled bytes behind a flush cost a sync"
    );
    assert!(
        after_awaited > after_blocking,
        "and the awaited flush pays the same bill"
    );
    println!(
        "flush: {before} syncs before, {after_blocking} after the blocking flush, \
         {after_awaited} after the awaited one"
    );

    Ok(())
}
