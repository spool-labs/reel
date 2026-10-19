//! A queue whose rows are put and deleted between slice writes, the way a node's repair queues churn
//!
//! The walks a node runs every second must not step over the deleted rows.

use tempfile::TempDir;

use reel::{
    ByteCount, Codec, ColumnId, ColumnSet, ColumnSpec, KeyWidth, ReelConfig, ReelStore, SyncPolicy,
    ThreadBudget,
};
use reel_core::{Direction, Store};

const QUEUE: &str = "queue";
const SLICES: &str = "slices";

/// A queue keyed like a node's pending repairs, a two-byte spool then a 32-byte address, beside its slices
const COLUMNS: ColumnSet = &[
    ColumnSpec {
        id: ColumnId(1),
        name: QUEUE,
        key_width: KeyWidth::Fixed(34),
        shard_bytes: 2,
        purge_mark: None,
        codec: Codec::None,
    },
    ColumnSpec {
        id: ColumnId(2),
        name: SLICES,
        key_width: KeyWidth::Fixed(34),
        shard_bytes: 2,
        purge_mark: None,
        codec: Codec::None,
    },
];

/// Queue rows put and deleted, one per slice written
const CHURN: u32 = 20_000;

/// Bytes in each slice, enough that the queue's rows spread across many segments
const SLICE_BYTES: usize = 4 * 1024;

/// Maintenance runs once per this many churned rows, the way a node ticks
const TICK: u32 = 500;

/// A bounded walk steps this many sealed keys or fewer past its prefix
const FEW: u64 = 64;

/// An unbounded walk may step over the deleted rows of the young segments no merge has reached yet,
/// a share that stays the same however long the queue has churned
const YOUNG: u64 = CHURN as u64 / 20;

fn config() -> ReelConfig {
    ReelConfig {
        segment_bytes: ByteCount::from_bytes(512 * 1024),
        sync: SyncPolicy::Never,
        active_tails: ThreadBudget::threads(1),
        scrub_mbps: 0,
        ..ReelConfig::default()
    }
}

/// A spool, then an address spread the way track hashes are, so segments overlap and runs merge
fn key(spool: u16, row: u32) -> Vec<u8> {
    let mut key = spool.to_be_bytes().to_vec();
    let mut state = u64::from(row).wrapping_add(0x9E37_79B9_7F4A_7C15);
    for _ in 0..4 {
        state = (state ^ (state >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        state = (state ^ (state >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        state ^= state >> 31;
        key.extend_from_slice(&state.to_be_bytes());
    }
    key
}

/// One live row in spool 1, then spool 5's rows churned through between slice writes, then a reopen
fn churned_queue(dir: &TempDir) -> ReelStore {
    let store = ReelStore::open(dir.path().to_path_buf(), config(), COLUMNS).expect("open");
    Store::put(&store, QUEUE, &key(1, 0), b"ready").expect("put live");
    let slice = vec![0x5a; SLICE_BYTES];
    for row in 0..CHURN {
        Store::put(&store, QUEUE, &key(5, row), b"ready").expect("queue");
        Store::put(&store, SLICES, &key(5, row), &slice).expect("slice");
        Store::delete(&store, QUEUE, &key(5, row)).expect("done");
        if row % TICK == TICK - 1 {
            store.maintain_once().expect("tick");
        }
    }
    store.maintain_once().expect("tick");
    store.close().expect("close");
    drop(store);
    ReelStore::open(dir.path().to_path_buf(), config(), COLUMNS).expect("reopen")
}

/// How many sealed keys walks stepped over since a reading
fn stepped_since(store: &ReelStore, before: u64) -> u64 {
    store.filter_probes().stepped - before
}

// the next spool with queued rows is found without stepping over the deleted ones
#[test]
fn next_spool() {
    let dir = TempDir::new().expect("tempdir");
    let store = churned_queue(&dir);
    let before = store.filter_probes().stepped;

    let next = Store::iter_from(&store, QUEUE, &key(2, 0), Direction::Asc)
        .expect("iter_from")
        .next();

    assert!(next.is_none());
    let stepped = stepped_since(&store, before);
    assert!(
        stepped <= YOUNG,
        "finding no next spool stepped over {stepped} keys"
    );
}

// every churned row stays gone once merges leave its delete out
#[test]
fn churned_gone() {
    let dir = TempDir::new().expect("tempdir");
    let store = churned_queue(&dir);

    let walked = Store::iter_prefix(&store, QUEUE, &5u16.to_be_bytes())
        .expect("iter_prefix")
        .count();
    let mut found = 0;
    for row in (0..CHURN).step_by(97) {
        if Store::get(&store, QUEUE, &key(5, row))
            .expect("get")
            .is_some()
        {
            found += 1;
        }
    }

    assert_eq!((walked, found), (0, 0));
}

// a spool's queue is walked without stepping into the next spool's deleted rows
#[test]
fn spool_walk() {
    let dir = TempDir::new().expect("tempdir");
    let store = churned_queue(&dir);
    let before = store.filter_probes().stepped;

    let walked: Vec<Vec<u8>> = Store::iter_prefix(&store, QUEUE, &1u16.to_be_bytes())
        .expect("iter_prefix")
        .map(|(key, _)| key)
        .collect();

    assert_eq!(walked, vec![key(1, 0)]);
    let stepped = stepped_since(&store, before);
    assert!(
        stepped <= FEW,
        "one spool's walk stepped over {stepped} keys"
    );
}

// asking whether a spool has queued rows steps over nothing
#[test]
fn spool_count() {
    let dir = TempDir::new().expect("tempdir");
    let store = churned_queue(&dir);
    let before = store.filter_probes().stepped;

    let churned = Store::count_prefix(&store, QUEUE, &5u16.to_be_bytes()).expect("count");
    let live = Store::count_prefix(&store, QUEUE, &1u16.to_be_bytes()).expect("count");

    assert_eq!((churned, live), (0, 1));
    assert_eq!(stepped_since(&store, before), 0);
}

// walks that keep stepping over deleted keys make a merge due, and the merge leaves them out
#[test]
fn buried_walks_merge_away() {
    let dir = TempDir::new().expect("tempdir");
    let store = churned_queue(&dir);
    let walk = |store: &ReelStore| {
        let before = store.filter_probes().buried;
        let next = Store::iter_from(store, QUEUE, &key(2, 0), Direction::Asc)
            .expect("iter_from")
            .next();
        assert!(next.is_none());
        store.filter_probes().buried - before
    };

    let first = walk(&store);
    assert!(
        first > 0,
        "the churn left no deleted keys for a walk to step over"
    );
    let mut buried = first;
    while buried < 1 << 20 {
        buried += walk(&store);
    }
    store.maintain_once().expect("tick");
    let after = walk(&store);
    assert_eq!(
        after, 0,
        "a walk still stepped over {after} deleted keys after the merge"
    );
}
