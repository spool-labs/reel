//! Interleavings pinned by name, so yesterday's races fail tomorrow's commit
//!
//! Each test parks a thread at a named point in another plane, moves the
//! volume while it stands there, and asserts the answer the engine promises.

use std::collections::BTreeSet;
use std::ops::Bound;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;

use reel_core::{Store, WriteBatch};

use reel::io::fault::FaultPlan;
use reel::io::sim_backend::SimIo;
use reel::sync::rendezvous;
use reel::{
    ByteCount, Codec, ColumnId, ColumnSet, ColumnSpec, CompactPass, IndexResidency, KeyPage,
    KeyWidth, MapShape, PlaybackCursor, Preallocate, RecordKey, ReelConfig, ReelStore, SyncPolicy,
    ThreadBudget, Way,
};

const COLUMNS: ColumnSet = &[ColumnSpec {
    id: ColumnId(1),
    name: "rows",
    key_width: KeyWidth::Fixed(8),
    shard_bytes: 0,
    inline_max: 0,
    row_carry: 0,
    purge_mark: None,
    codec: Codec::None,
    map_shape: MapShape::Tree,
}];

fn config(index: IndexResidency) -> ReelConfig {
    ReelConfig {
        segment_bytes: ByteCount::from_bytes(128 * 1024),
        alloc_chunk: ByteCount::from_bytes(32 * 1024),
        preallocate: Preallocate::Chunk,
        sync: SyncPolicy::Never,
        active_tails: ThreadBudget::threads(1),
        index,
        ..ReelConfig::default()
    }
}

/// The same column shared into 256 shards, so a page fill crosses more than one
///
/// A page reads a shard at a time and gives its lock up between them, which is the
/// only seam a batch can land in the middle of a fill through.
const SHARDED: ColumnSet = &[ColumnSpec {
    id: ColumnId(1),
    name: "rows",
    key_width: KeyWidth::Fixed(8),
    shard_bytes: 1,
    inline_max: 0,
    row_carry: 0,
    purge_mark: None,
    codec: Codec::None,
    map_shape: MapShape::Tree,
}];

fn open(root: &str, seed: u64, index: IndexResidency) -> Arc<ReelStore> {
    open_columns(root, seed, index, COLUMNS)
}

fn open_columns(
    root: &str,
    seed: u64,
    index: IndexResidency,
    columns: ColumnSet,
) -> Arc<ReelStore> {
    let store = ReelStore::open_with_io(
        PathBuf::from(root),
        config(index),
        columns,
        Arc::new(SimIo::new(FaultPlan::new(seed))),
    )
    .expect("open");
    Arc::new(store)
}

/// A key in a named shard, since the shard is the leading byte
fn sharded_key(shard: u8, at: u8) -> [u8; 8] {
    let mut key = [0u8; 8];
    key[0] = shard;
    key[1] = at;
    key
}

// a delete that lands while compaction stands between its copy and its repoint wins
#[test]
fn a_delete_in_the_repoint_window_wins() {
    let store = open("/repoint-race", 41, IndexResidency::Resident);
    let payload = vec![0x2Du8; 4096];
    for at in 0..60u64 {
        Store::put(&*store, "rows", &at.to_be_bytes(), &payload).expect("put");
    }
    store.flush().expect("flush");
    // Everything but key 5 dies, so the pass has exactly one live record to copy.
    for at in (0..60u64).filter(|at| *at != 5) {
        Store::delete(&*store, "rows", &at.to_be_bytes()).expect("delete");
    }

    let script = rendezvous::script();
    script.hold("compaction/repoint");

    let compactor = {
        let store = Arc::clone(&store);
        thread::spawn(move || store.compact_once().expect("compact"))
    };
    script.await_reached("compaction/repoint", 1);

    // The copy of key 5 is down and unrepointed, and this delete draws a newer sequence
    // number, so the repoint waking up must find its version gone.
    Store::delete(&*store, "rows", &5u64.to_be_bytes()).expect("delete");

    script.release("compaction/repoint");
    compactor.join().expect("compaction thread");

    assert!(
        Store::get(&*store, "rows", &5u64.to_be_bytes())
            .expect("get")
            .is_none(),
        "the repoint resurrected a key deleted in its window"
    );
}

// keys stay readable while a seal is durable but the index has not been told
#[test]
fn every_key_answers_while_a_seal_waits_to_be_queued() {
    let store = open("/seal-race", 43, IndexResidency::Resident);
    let script = rendezvous::script();
    script.hold("seal/queued");

    // Enough records to roll the first segment, whose seal then parks on the way to the
    // queue. The writes must not block on it.
    let payload = vec![0x4Bu8; 4096];
    for at in 0..40u64 {
        Store::put(&*store, "rows", &at.to_be_bytes(), &payload).expect("put");
    }
    script.await_reached("seal/queued", 1);

    for at in 0..40u64 {
        assert!(
            Store::get(&*store, "rows", &at.to_be_bytes())
                .expect("get")
                .is_some(),
            "key {at} vanished while a seal stood unqueued"
        );
    }

    script.release("seal/queued");
    store.flush().expect("flush");
    for at in 0..40u64 {
        assert!(
            Store::get(&*store, "rows", &at.to_be_bytes())
                .expect("get")
                .is_some(),
            "key {at} vanished after the seal was picked up"
        );
    }
}

// a read mid-handover sees every key, from the map or from a footer
#[test]
fn every_key_answers_between_two_paged_handovers() {
    let store = open("/handover-race", 47, IndexResidency::Paged);
    let payload = vec![0x71u8; 4096];
    for at in 0..200u64 {
        Store::put(&*store, "rows", &at.to_be_bytes(), &payload).expect("put");
    }
    store.flush().expect("flush");

    let script = rendezvous::script();
    script.hold("paged/handover");

    let pager = {
        let store = Arc::clone(&store);
        thread::spawn(move || store.page_out_sealed().expect("page out"))
    };

    // At the second arrival the first segment's keys are footer-answered and the rest
    // are still resident.
    script.await_reached("paged/handover", 1);
    script.pass_one("paged/handover");
    script.await_reached("paged/handover", 2);

    for at in 0..200u64 {
        assert!(
            Store::get(&*store, "rows", &at.to_be_bytes())
                .expect("get")
                .is_some(),
            "key {at} vanished mid-handover"
        );
    }

    script.release("paged/handover");
    let paged = pager.join().expect("pager thread");
    assert!(paged > 0, "the pass handed nothing over");

    for at in 0..200u64 {
        assert!(
            Store::get(&*store, "rows", &at.to_be_bytes())
                .expect("get")
                .is_some(),
            "key {at} vanished after the handover"
        );
    }
}

// a whole-column page racing a batch never comes back holding half of it
#[test]
fn a_page_never_comes_back_holding_half_a_batch() {
    let store = open_columns("/optimistic-page", 53, IndexResidency::Resident, SHARDED);
    for shard in [0x00u8, 0x80] {
        Store::put(&*store, "rows", &sharded_key(shard, 0), &[0x11; 32]).expect("put");
    }

    let script = rendezvous::script();
    script.hold("index/page-shard");

    let reader = {
        let store = Arc::clone(&store);
        thread::spawn(move || {
            let mut playback =
                PlaybackCursor::new(SHARDED[0].id, Way::Up, Bound::Unbounded).expect("playback");
            let mut page = KeyPage::with_lens();
            store.page_from(&mut playback, 16, &mut page).expect("page");
            (0..page.len())
                .map(|at| page.key_at(at))
                .collect::<Vec<_>>()
        })
    };

    // Parked at the head of the high shard, holding no shard lock, which is what lets the
    // batch below land at all.
    script.await_reached("index/page-shard", 1);
    script.pass_one("index/page-shard");
    script.await_reached("index/page-shard", 2);

    let mut batch = WriteBatch::new();
    batch.put("rows", &sharded_key(0x00, 1), &[0x22; 32]);
    batch.put("rows", &sharded_key(0x80, 1), &[0x22; 32]);
    Store::write_batch(&*store, batch).expect("batch");

    script.release("index/page-shard");
    let seen = reader.join().expect("reader thread");

    let held = |key: [u8; 8]| seen.iter().any(|found| found[..] == key[..]);
    assert!(
        held(sharded_key(0x00, 1)) && held(sharded_key(0x80, 1)),
        "a page filled across a batch kept part of it: {seen:?}"
    );
    assert_eq!(seen.len(), 4, "the refilled page missed a key: {seen:?}");
}

/// A volume with small segments and its simulator, for a test that reads the image
///
/// The image is what says whether a segment is still there.
fn open_sim(root: &str, seed: u64) -> (Arc<ReelStore>, SimIo) {
    let sim = SimIo::new(FaultPlan::new(seed));
    let store = ReelStore::open_with_io(
        PathBuf::from(root),
        ReelConfig {
            segment_bytes: ByteCount::from_bytes(65_536),
            alloc_chunk: ByteCount::from_bytes(4_096),
            preallocate: Preallocate::Chunk,
            sync: SyncPolicy::Never,
            active_tails: ThreadBudget::threads(1),
            index: IndexResidency::Resident,
            ..ReelConfig::default()
        },
        COLUMNS,
        Arc::new(sim.clone()),
    )
    .expect("open");
    (Arc::new(store), sim)
}

/// The number of the segment this path names, for a file in a durable image
fn segment_number_of(path: &std::path::Path) -> Option<u32> {
    path.file_name()?
        .to_str()?
        .strip_suffix(".reel")?
        .parse()
        .ok()
}

// a pass on a stale pending list must not retire a segment whose spans are in flight
#[test]
fn a_late_note_cannot_bring_back_a_retired_segment() {
    let (store, sim) = open_sim("/late-note", 61);
    let payload = vec![0x5Au8; 1_000];
    let put = |at: u64| Store::put(&*store, "rows", &at.to_be_bytes(), &payload).expect("put");
    let segments = |sim: &SimIo| -> BTreeSet<u32> {
        sim.durable_image()
            .iter()
            .filter_map(|(path, _)| segment_number_of(path))
            .collect()
    };

    // Roll the tail once. Counted rather than computed: how many records a segment holds
    // is a property of the framing, and a hard-coded count stops rolling the day it moves.
    let mut next = 1u64;
    while segments(&sim).len() < 2 {
        put(next);
        next += 1;
    }

    // Dead bytes in the segment still being written to and none in the sealed one, so the
    // sealed one cannot be what the pass takes.
    let dirtied = next;
    for _ in 0..10 {
        put(next);
        next += 1;
    }
    // Twice, so more than half of what the segment holds is dead: the ranked selection
    // takes the best segment past the plane's ratio, which is half.
    for _ in 0..2 {
        for at in dirtied..next {
            put(at);
        }
    }
    assert_eq!(
        segments(&sim).len(),
        2,
        "the tail rolled while it was being dirtied"
    );
    // A read settles the queue, so the pass below is entitled to the sealed segment.
    assert!(Store::get(&*store, "rows", &1u64.to_be_bytes())
        .expect("get")
        .is_some());

    let script = rendezvous::script();
    script.hold("compaction/owed");

    let compactor = {
        let store = Arc::clone(&store);
        thread::spawn(move || store.compact_once().expect("compact"))
    };
    // Twice: the drain asks for wholly dead segments and finds none, then the ranked
    // selection asks. The second copy is the one this is about.
    script.await_reached("compaction/owed", 1);
    script.pass_one("compaction/owed");
    script.await_reached("compaction/owed", 2);

    // The tail rolls while the pass stands on its copies, so the segment it seals is in
    // the copied ranking as a live tail and not in the copied queue.
    while segments(&sim).len() < 3 {
        put(next);
        next += 1;
    }

    script.hold("seal/spans");
    // The tail opens the next segment before the sealer has queued the note for the one
    // it left, so a reader that asks once may ask too early.
    let stop = Arc::new(AtomicBool::new(false));
    let reader = {
        let store = Arc::clone(&store);
        let stop = Arc::clone(&stop);
        thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                Store::get(&*store, "rows", &1u64.to_be_bytes()).expect("get");
            }
        })
    };
    // The note holds the footer of the segment that just sealed, with no span recorded.
    script.await_reached("seal/spans", 1);

    let before = segments(&sim);
    script.release("compaction/owed");
    let pass = compactor.join().expect("compaction thread");
    assert_eq!(
        pass,
        CompactPass::Idle,
        "the pass took a segment whose spans had not reached the index"
    );
    assert_eq!(segments(&sim), before, "an idle pass retired a segment");

    script.release("seal/spans");
    stop.store(true, Ordering::Relaxed);
    reader.join().expect("reader thread");

    let standing = segments(&sim);
    let mut phantom = BTreeSet::new();
    for at in 1..next {
        let key = RecordKey::from_bytes(COLUMNS[0].id, &at.to_be_bytes()).expect("key");
        for segment in &store.index().sites(&key).expect("sites").candidates {
            if !standing.contains(&segment.as_u32()) {
                phantom.insert(segment.as_u32());
            }
        }
    }
    assert!(
        phantom.is_empty(),
        "the live store still searches retired segments {phantom:?}, standing {standing:?}"
    );

    // The pass above declined a real target rather than finding none, and the same
    // segment goes once its spans are down.
    assert_eq!(store.compact_once().expect("compact"), CompactPass::Copied);
    assert!(
        segments(&sim).len() < standing.len(),
        "the segment the guard held back was never a target: {standing:?}"
    );
}
