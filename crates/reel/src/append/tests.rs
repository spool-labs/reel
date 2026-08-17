//! Append-path tests over the simulated backend

use super::*;

use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Barrier;
use std::task::{Context, Poll, Waker};
use std::thread;

use crate::units::ByteCount;

use crate::append::admission::InflightBudget;
use crate::config::{IoBackend, Preallocate, ReelConfig, DEFAULT_FD_CACHE};
use crate::format::column::{Codec, ColumnId, ColumnSet, ColumnSpec, KeyWidth, MapShape};
use crate::format::footer::SegmentFooter;
use crate::format::loc::SegmentId;
use crate::format::segment_header::SEGMENT_HEADER_LEN;
use crate::io::fault::{FaultKind, FaultPlan};
use crate::io::op::{Advice, SegmentEntry};
use crate::io::sim_backend::SimIo;
use crate::reel::segment::{FdCache, IoDriver};
use crate::sync::tension::block_on;

const REEL_DIR: &str = "/bulk";
const RECORDS: ColumnId = ColumnId(1);
const KEY_WIDTH: usize = 34;
const SEG_HEADER_SPAN: u64 = HEADER_LEN as u64 + SEGMENT_HEADER_LEN as u64;

/// One fixed-key column, the shape the append path is exercised over
const COLUMNS: ColumnSet = &[ColumnSpec {
    id: RECORDS,
    name: "records",
    key_width: KeyWidth::Fixed(KEY_WIDTH as u16),
    shard_bytes: 2,
    inline_max: 0,
    row_carry: 0,
    purge_mark: None,
    codec: Codec::None,
    map_shape: MapShape::Tree,
}];

fn config(sync: SyncPolicy, preallocate: Preallocate) -> ReelConfig {
    ReelConfig {
        segment_bytes: ByteCount::mb(1),
        alloc_chunk: ByteCount::from_bytes(ALIGN * 4),
        preallocate,
        sync,
        ..ReelConfig::default()
    }
}

fn harness(config: ReelConfig, plan: FaultPlan) -> (Arc<ReelShared>, SimIo) {
    harness_capped(config, plan, InflightBudget::default())
}

/// The same harness under a named admission ceiling, for the backpressure cells
fn harness_capped(
    config: ReelConfig,
    plan: FaultPlan,
    budget: InflightBudget,
) -> (Arc<ReelShared>, SimIo) {
    let sim = SimIo::new(plan);
    let driver = Arc::new(IoDriver::new(Arc::new(sim.clone())));
    let budget = Arc::new(budget);
    let fd_cache = Arc::new(FdCache::new(DEFAULT_FD_CACHE as usize));
    let shared = Arc::new(ReelShared::new(
        PathBuf::from(REEL_DIR),
        driver,
        budget,
        fd_cache,
        config,
        COLUMNS,
        1,
    ));
    (shared, sim)
}

fn key(byte: u8) -> RecordKey {
    RecordKey::from_bytes(RECORDS, &[byte; KEY_WIDTH]).expect("key")
}

/// Poll a wait once with a waker that does nothing, for the waits that must not
fn poll_once<Awaited: Future>(future: Pin<&mut Awaited>) -> Poll<Awaited::Output> {
    future.poll(&mut Context::from_waker(Waker::noop()))
}

/// Bytes a record with a key of this column's width and this payload takes
fn framed(payload_len: usize) -> u64 {
    HEADER_LEN as u64 + KEY_WIDTH as u64 + payload_len as u64
}

fn read_segment(shared: &ReelShared, path: &Path) -> Vec<u8> {
    let dir = path.parent().expect("parent");
    let name = path
        .file_name()
        .expect("name")
        .to_string_lossy()
        .into_owned();
    let entries = shared.driver.list(dir).expect("list");
    let len = entry_len(&entries, &name);
    let file = shared.driver.open(path, false).expect("open");
    shared.driver.pread(file, 0, len).expect("read")
}

fn entry_len(entries: &[SegmentEntry], name: &str) -> u64 {
    entries
        .iter()
        .find(|entry| entry.name == name)
        .map(|entry| entry.len)
        .expect("segment listed")
}

fn walk(bytes: &[u8], limit: u64) -> Vec<(RecordHeader, u64)> {
    let mut out = Vec::new();
    let mut offset = 0u64;
    while offset + HEADER_LEN as u64 <= limit {
        let header = RecordHeader::unpack(&bytes[offset as usize..]).expect("header");
        let next = offset + header.span();
        out.push((header, offset));
        offset = next;
    }
    out
}

// opening a tail writes the segment header as record zero and nothing else
#[test]
fn open_writes_segment_header() {
    let (shared, _sim) = harness(
        config(SyncPolicy::EveryPut, Preallocate::Chunk),
        FaultPlan::new(1),
    );
    let appender = Appender::open(Arc::clone(&shared), 0).expect("open");

    assert_eq!(appender.tail().active_segment(), SegmentId(1));
    assert_eq!(appender.tail().committed_len(), SEG_HEADER_SPAN);

    let bytes = read_segment(&shared, &shared.segment_path(SegmentId(1)));
    let records = walk(&bytes, SEG_HEADER_SPAN);
    assert_eq!(records.len(), 1);
    assert!(records[0].0.flags.is_segment_header());
    assert_eq!(records[0].1, 0);
}

// a whole-block volume closes the header drain with a pad to the boundary
#[test]
fn whole_block_open_pads_to_boundary() {
    let mut settings = config(SyncPolicy::EveryPut, Preallocate::Chunk);
    settings.io_backend = IoBackend::UringDirect;
    let (shared, _sim) = harness(settings, FaultPlan::new(1));
    let appender = Appender::open(Arc::clone(&shared), 0).expect("open");

    assert_eq!(appender.tail().committed_len(), ALIGN);

    let bytes = read_segment(&shared, &shared.segment_path(SegmentId(1)));
    let records = walk(&bytes, ALIGN);
    assert!(records[0].0.flags.is_segment_header());
    assert!(records[1].0.flags.is_pad());
}

// records land back to back after the segment header, each where it reserved
#[test]
fn records_land_contiguously() {
    let (shared, _sim) = harness(
        config(SyncPolicy::Never, Preallocate::Chunk),
        FaultPlan::new(1),
    );
    let appender = Appender::open(Arc::clone(&shared), 0).expect("open");

    for (byte, len) in [(1u8, 100usize), (2, 200), (3, 300)] {
        appender
            .append_data(key(byte), vec![0xa0 + byte; len], 0, Commit::PerRecord)
            .expect("append");
    }

    let committed = appender.tail().committed_len();
    assert_eq!(
        committed,
        SEG_HEADER_SPAN + framed(100) + framed(200) + framed(300)
    );

    let bytes = read_segment(&shared, &shared.segment_path(SegmentId(1)));
    let records = walk(&bytes, committed);
    let data: Vec<&(RecordHeader, u64)> = records
        .iter()
        .filter(|(header, _)| header.flags.is_data())
        .collect();
    assert_eq!(data.len(), 3);
    assert_eq!(data[0].1, SEG_HEADER_SPAN);
    assert_eq!(data[1].1, SEG_HEADER_SPAN + framed(100));
    assert_eq!(data[2].1, SEG_HEADER_SPAN + framed(100) + framed(200));
    let pads = records
        .iter()
        .filter(|(header, _)| header.flags.is_pad())
        .count();
    assert_eq!(pads, 0);
}

// every drain of a whole-block volume leaves the write head on a boundary
#[test]
fn every_whole_block_drain_is_aligned() {
    let mut settings = config(SyncPolicy::Never, Preallocate::Chunk);
    settings.io_backend = IoBackend::UringDirect;
    let (shared, _sim) = harness(settings, FaultPlan::new(1));
    let appender = Appender::open(Arc::clone(&shared), 0).expect("open");

    for byte in 1..=5u8 {
        appender
            .append_data(
                key(byte),
                vec![byte; byte as usize * 111],
                0,
                Commit::PerRecord,
            )
            .expect("append");
        assert_eq!(appender.tail().committed_len() % ALIGN, 0);
    }
}

// a buffered drain leaves the write head at the end of its own records
#[test]
fn buffered_drain_writes_only_its_records() {
    let (shared, _sim) = harness(
        config(SyncPolicy::Never, Preallocate::Chunk),
        FaultPlan::new(1),
    );
    let appender = Appender::open(Arc::clone(&shared), 0).expect("open");

    let mut expected = SEG_HEADER_SPAN;
    for byte in 1..=5u8 {
        let payload = byte as u64 * 111;
        appender
            .append_data(
                key(byte),
                vec![byte; payload as usize],
                0,
                Commit::PerRecord,
            )
            .expect("append");
        expected += framed(payload as usize);
        assert_eq!(appender.tail().committed_len(), expected);
    }
}

// a sync leaves the write head exactly where the records left it
#[test]
fn a_sync_adds_no_bytes() {
    let (shared, sim) = harness(
        config(SyncPolicy::EveryPut, Preallocate::Chunk),
        FaultPlan::new(1),
    );
    let appender = Appender::open(Arc::clone(&shared), 0).expect("open");

    appender
        .append_data(key(1), vec![0x11; 500], 0, Commit::PerRecord)
        .expect("first");

    // Durability is the flush and nothing else: no bookkeeping is written for it.
    let record_end = SEG_HEADER_SPAN + framed(500);
    assert_eq!(appender.tail().committed_len(), record_end);
    assert!(sim.sync_count() > 0, "the put reached the device");

    let bytes = read_segment(&shared, &shared.segment_path(SegmentId(1)));
    let records = walk(&bytes, appender.tail().committed_len());
    assert_eq!(
        records.len(),
        2,
        "a segment header and the record, nothing else"
    );
}

// a flush makes what has settled durable without adding a record for it
#[test]
fn flush_syncs_what_settled() {
    let (shared, sim) = harness(
        config(SyncPolicy::Never, Preallocate::Chunk),
        FaultPlan::new(1),
    );
    let appender = Appender::open(Arc::clone(&shared), 0).expect("open");

    appender
        .append_data(key(1), vec![0x11; 500], 0, Commit::PerRecord)
        .expect("append");
    let record_end = appender.tail().committed_len();
    let before = sim.sync_count();

    appender.flush().expect("flush");

    assert!(sim.sync_count() > before, "the flush reached the device");
    let bytes = read_segment(&shared, &shared.segment_path(SegmentId(1)));
    let records = walk(&bytes, appender.tail().committed_len());
    assert_eq!(
        appender.tail().committed_len(),
        record_end,
        "the flush wrote no record of its own"
    );
    assert!(records
        .iter()
        .all(|(header, _)| !header.flags.is_pad() || header.length > 0));
}

// a tail with no sync owed answers the awaitable wait without waiting
#[test]
fn nothing_owed_settles_at_once() {
    let (shared, sim) = harness(
        config(SyncPolicy::Never, Preallocate::Chunk),
        FaultPlan::new(1),
    );
    let appender = Appender::open(Arc::clone(&shared), 0).expect("open");

    appender
        .append_data(key(1), vec![0x11; 500], 0, Commit::Batched)
        .expect("append");
    let before = sim.sync_count();

    let settled = block_on(appender.owed_turn()).expect("wait");

    assert!(matches!(settled, Durability::Settled));
    assert_eq!(
        sim.sync_count(),
        before,
        "a wait that owes nothing asks the device nothing"
    );
}

// the wait hands back the turn rather than taking the device on its own thread
#[test]
fn an_owed_sync_hands_back_the_turn() {
    let (shared, sim) = harness(
        config(SyncPolicy::EveryPut, Preallocate::Chunk),
        FaultPlan::new(1),
    );
    let appender = Appender::open(Arc::clone(&shared), 0).expect("open");

    appender
        .append_data(key(1), vec![0x11; 500], 0, Commit::Batched)
        .expect("append");
    let before = sim.sync_count();

    let owed = block_on(appender.owed_turn()).expect("wait");
    let Durability::Owed(turn) = owed else {
        panic!("a sync was owed, so the turn belongs to this caller");
    };
    assert_eq!(
        sim.sync_count(),
        before,
        "the wait itself never reached the device"
    );

    appender.take_turn(turn).expect("turn");

    assert!(
        sim.sync_count() > before,
        "the turn is what reached the device"
    );
    let settled = block_on(appender.owed_turn()).expect("second wait");
    assert!(matches!(settled, Durability::Settled));
}

// a writer waiting on another writer's flush holds no turn and no thread
#[test]
fn a_second_writer_waits_on_the_first() {
    let (shared, sim) = harness(
        config(SyncPolicy::EveryPut, Preallocate::Chunk),
        FaultPlan::new(1),
    );
    let appender = Appender::open(Arc::clone(&shared), 0).expect("open");

    appender
        .append_data(key(1), vec![0x11; 500], 0, Commit::Batched)
        .expect("append");
    let before = sim.sync_count();

    let mut first = Box::pin(appender.owed_turn());
    let Poll::Ready(Ok(Durability::Owed(turn))) = poll_once(first.as_mut()) else {
        panic!("the first writer finds the device free");
    };

    let mut second = Box::pin(appender.owed_turn());
    assert!(
        poll_once(second.as_mut()).is_pending(),
        "the second writer waits on the first"
    );
    assert_eq!(
        sim.sync_count(),
        before,
        "neither writer has reached the device yet"
    );

    appender.take_turn(turn).expect("turn");

    let Poll::Ready(Ok(Durability::Settled)) = poll_once(second.as_mut()) else {
        panic!("one flush answers for both writers");
    };
    assert_eq!(sim.sync_count(), before + 1, "one flush, not two");
}

// a turn nobody takes goes back, so the writer behind it is not left waiting
#[test]
fn a_dropped_turn_frees_the_device() {
    let (shared, _sim) = harness(
        config(SyncPolicy::EveryPut, Preallocate::Chunk),
        FaultPlan::new(1),
    );
    let appender = Appender::open(Arc::clone(&shared), 0).expect("open");

    appender
        .append_data(key(1), vec![0x11; 500], 0, Commit::Batched)
        .expect("append");

    let owed = block_on(appender.owed_turn()).expect("wait");
    assert!(matches!(owed, Durability::Owed(_)));
    drop(owed);

    let mut next = Box::pin(appender.owed_turn());
    let Poll::Ready(Ok(Durability::Owed(_))) = poll_once(next.as_mut()) else {
        panic!("the turn is free again");
    };
}

// a turn the async door draws is run by the sealer and answers the caller
#[test]
fn a_forwarded_turn_settles_the_segment() {
    let (shared, sim) = harness(
        config(SyncPolicy::EveryPut, Preallocate::Chunk),
        FaultPlan::new(1),
    );
    let appender = Appender::open(Arc::clone(&shared), 0).expect("open");

    appender
        .append_data(key(1), vec![0x11; 500], 0, Commit::Batched)
        .expect("append");
    let before = sim.sync_count();

    block_on(appender.sync_if_owed_wait()).expect("forwarded sync");

    assert!(
        sim.sync_count() > before,
        "the forwarded turn reached the device"
    );
    let settled = block_on(appender.owed_turn()).expect("second wait");
    assert!(matches!(settled, Durability::Settled));
}

// one forwarded flush answers the writer that drew it and the one behind it
#[test]
fn a_forwarded_flush_answers_both_writers() {
    let (shared, sim) = harness(
        config(SyncPolicy::EveryPut, Preallocate::Chunk),
        FaultPlan::new(1),
    );
    let appender = Appender::open(Arc::clone(&shared), 0).expect("open");

    appender
        .append_data(key(1), vec![0x11; 500], 0, Commit::Batched)
        .expect("append");
    let before = sim.sync_count();

    let mut first = Box::pin(appender.sync_if_owed_wait());
    // The poll that draws the turn and hands it over. What it answers depends on whether
    // the sealer got there first, which is a race this door has by design.
    let handed = poll_once(first.as_mut());
    block_on(appender.sync_if_owed_wait()).expect("second writer");
    match handed {
        Poll::Ready(answered) => answered.expect("first writer"),
        Poll::Pending => block_on(first).expect("first writer"),
    }

    assert_eq!(sim.sync_count(), before + 1, "one flush, not two");
}

// a caller that walks away from a forwarded flush leaves the turn where it is
#[test]
fn a_dropped_wait_keeps_the_flush() {
    let (shared, sim) = harness(
        config(SyncPolicy::EveryPut, Preallocate::Chunk),
        FaultPlan::new(1),
    );
    let appender = Appender::open(Arc::clone(&shared), 0).expect("open");

    appender
        .append_data(key(1), vec![0x11; 500], 0, Commit::Batched)
        .expect("append");
    let before = sim.sync_count();

    {
        let mut walked = Box::pin(appender.sync_if_owed_wait());
        let _ = poll_once(walked.as_mut());
    }

    // The sealer holds the turn now, so this waits for it rather than hanging.
    appender.sync_if_owed().expect("blocking sync");
    assert_eq!(
        sim.sync_count(),
        before + 1,
        "the abandoned flush is the one that ran"
    );
}

// an awaited append waits for admission rather than holding a thread for it
#[test]
fn an_awaited_append_waits_for_room() {
    let (shared, _sim) = harness_capped(
        config(SyncPolicy::Never, Preallocate::Chunk),
        FaultPlan::new(1),
        InflightBudget::new(ByteCount::from_bytes(4096)),
    );
    let appender = Appender::open(Arc::clone(&shared), 0).expect("open");
    shared.budget.acquire(4096);

    let mut waiting =
        Box::pin(appender.append_data_wait(key(1), vec![0x11; 500], 0, Commit::Batched));
    assert!(
        poll_once(waiting.as_mut()).is_pending(),
        "no room, so no record"
    );
    shared.budget.release(4096);

    block_on(waiting).expect("append");
    assert_eq!(shared.budget.queued_bytes(), ByteCount::from_bytes(0));
}

// an append dropped at either of its waits gives its admission back
#[test]
fn a_dropped_append_holds_no_admission() {
    let (shared, _sim) = harness_capped(
        config(SyncPolicy::EveryPut, Preallocate::Chunk),
        FaultPlan::new(1),
        InflightBudget::new(ByteCount::from_bytes(4096)),
    );
    let appender = Appender::open(Arc::clone(&shared), 0).expect("open");

    shared.budget.acquire(4096);
    {
        let mut waiting =
            Box::pin(appender.append_data_wait(key(1), vec![0x11; 500], 0, Commit::Batched));
        assert!(poll_once(waiting.as_mut()).is_pending());
    }
    shared.budget.release(4096);
    assert_eq!(
        shared.budget.queued_bytes(),
        ByteCount::from_bytes(0),
        "dropped at admission"
    );

    // And dropped at the other wait, with the record already on the device.
    appender
        .append_data(key(2), vec![0x22; 500], 0, Commit::Batched)
        .expect("append");
    let held = block_on(appender.owed_turn()).expect("turn");
    {
        let mut waiting =
            Box::pin(appender.append_data_wait(key(3), vec![0x33; 500], 0, Commit::PerRecord));
        assert!(
            poll_once(waiting.as_mut()).is_pending(),
            "the turn is taken"
        );
    }

    assert_eq!(
        shared.budget.queued_bytes(),
        ByteCount::from_bytes(0),
        "dropped at the sync"
    );
    assert_eq!(appender.load(), 0, "and counted out of the tail");
    drop(held);
}

/// The batch of three the framing cells write, each record the same width
fn batch_of(payload_len: usize) -> Vec<BatchRecord> {
    (1..=3u8)
        .map(|byte| BatchRecord {
            key: key(byte),
            write: BatchWrite::Put(vec![byte; payload_len], 0),
        })
        .collect()
}

/// The frame a walk found, with the records it declares
fn frame_of(records: &[(RecordHeader, u64)], bytes: &[u8]) -> (BatchFrame, usize) {
    let at = records
        .iter()
        .position(|(header, _)| header.flags.is_batch_frame())
        .expect("the batch wrote a frame");
    let (header, offset) = &records[at];
    let payload =
        &bytes[(offset + header.prefix_len()) as usize..(offset + header.span()) as usize];
    assert!(
        header.verify(payload),
        "the frame verifies against what it declares"
    );
    (
        BatchFrame::unpack(header, payload).expect("a frame declaration"),
        at,
    )
}

// a batch opens with a frame declaring exactly the run written behind it
#[test]
fn a_batch_is_framed() {
    let (shared, _sim) = harness(
        config(SyncPolicy::Never, Preallocate::Chunk),
        FaultPlan::new(1),
    );
    let appender = Appender::open(Arc::clone(&shared), 0).expect("open");

    appender.append_batch(batch_of(300)).expect("batch");

    let committed = appender.tail().committed_len();
    let bytes = read_segment(&shared, &shared.segment_path(SegmentId(1)));
    let records = walk(&bytes, committed);
    let (frame, at) = frame_of(&records, &bytes);

    assert_eq!(frame.count, 3);
    assert_eq!(frame.span, framed(300) * 3);
    assert_eq!(records[at].1, SEG_HEADER_SPAN, "the frame opens the run");
    let members = &records[at + 1..at + 1 + frame.count as usize];
    assert!(members.iter().all(|(header, _)| header.flags.is_batched()));
    let run: u64 = members.iter().map(|(header, _)| header.span()).sum();
    assert_eq!(run, frame.span, "the frame declares the bytes the run took");
    assert_eq!(committed, SEG_HEADER_SPAN + BatchFrame::SPAN + frame.span);
}

// a batch of one record is a plain record, framed and marked as nothing
#[test]
fn a_batch_of_one_pays_for_no_frame() {
    let (shared, _sim) = harness(
        config(SyncPolicy::Never, Preallocate::Chunk),
        FaultPlan::new(1),
    );
    let appender = Appender::open(Arc::clone(&shared), 0).expect("open");

    appender
        .append_batch(vec![BatchRecord {
            key: key(1),
            write: BatchWrite::Put(vec![0x11; 300], 0),
        }])
        .expect("batch");

    let committed = appender.tail().committed_len();
    assert_eq!(committed, SEG_HEADER_SPAN + framed(300));

    let bytes = read_segment(&shared, &shared.segment_path(SegmentId(1)));
    let records = walk(&bytes, committed);
    assert!(records
        .iter()
        .all(|(header, _)| !header.flags.is_batch_frame()));
    assert!(records.iter().all(|(header, _)| !header.flags.is_batched()));
}

// a whole-block volume closes a batch with a pad behind the run, not inside it
#[test]
fn a_whole_block_batch_pads_behind_its_run() {
    let mut settings = config(SyncPolicy::Never, Preallocate::Chunk);
    settings.io_backend = IoBackend::UringDirect;
    let (shared, _sim) = harness(settings, FaultPlan::new(1));
    let appender = Appender::open(Arc::clone(&shared), 0).expect("open");

    appender.append_batch(batch_of(300)).expect("batch");

    let committed = appender.tail().committed_len();
    assert_eq!(
        committed % ALIGN,
        0,
        "the batch left the head off a boundary"
    );

    let bytes = read_segment(&shared, &shared.segment_path(SegmentId(1)));
    let records = walk(&bytes, committed);
    let (frame, at) = frame_of(&records, &bytes);

    assert_eq!(frame.count, 3);
    let (closing, _) = &records[at + 1 + frame.count as usize];
    assert!(
        closing.flags.is_pad(),
        "the pad is not what follows the run"
    );
}

// a batch too wide for the room left rolls whole rather than splitting in two
//
// The reservation covers the frame and every record at once, so a batch that runs past
// the segment gives the whole range up and retakes it on the next one. That is what
// keeps a frame and its run in one file, which recovery walks one file at a time.
#[test]
fn a_batch_never_spans_segments() {
    let mut settings = config(SyncPolicy::Never, Preallocate::Chunk);
    settings.segment_bytes = ByteCount::from_bytes(64 * 1024);
    let (shared, _sim) = harness(settings, FaultPlan::new(1));
    let appender = Appender::open(Arc::clone(&shared), 0).expect("open");

    let payload = 4_000;
    let batch_span = BatchFrame::SPAN + framed(payload) * 3;
    let target = shared.config.segment_bytes.to_bytes();
    while target - appender.tail().committed_len() >= batch_span + ALIGN {
        appender
            .append_data(key(9), vec![0x99; payload], 0, Commit::PerRecord)
            .expect("fill");
    }
    let filled = appender.tail().active_segment();

    let committed = appender.append_batch(batch_of(payload)).expect("batch");

    let landed: Vec<SegmentId> = committed.iter().map(|record| record.loc.segment).collect();
    assert_ne!(
        landed[0], filled,
        "premise: the batch did not fit the segment it was offered"
    );
    assert!(
        landed.iter().all(|segment| *segment == landed[0]),
        "a batch landed across {landed:?}",
    );

    let bytes = read_segment(&shared, &shared.segment_path(landed[0]));
    let records = walk(&bytes, appender.tail().committed_len());
    let (frame, at) = frame_of(&records, &bytes);
    assert_eq!(frame.count, 3);
    assert_eq!(
        records[at + 1].1,
        records[at].1 + BatchFrame::SPAN,
        "the run follows its frame with nothing between",
    );
}

// the committed length reaches past every record a drain wrote
#[test]
fn committed_length_reaches_past_every_record() {
    let (shared, _sim) = harness(
        config(SyncPolicy::Never, Preallocate::Chunk),
        FaultPlan::new(1),
    );
    let appender = Appender::open(Arc::clone(&shared), 0).expect("open");

    let committed = appender
        .append_data(key(9), vec![0x99; 700], 0, Commit::PerRecord)
        .expect("append");
    let record_end = committed.loc.offset as u64 + HEADER_LEN as u64 + 700;
    assert!(appender.tail().committed_len() >= record_end);
}

// a writer that finds the spare being drawn walks away rather than queueing
#[test]
fn drawing_a_spare_never_queues_a_writer() {
    let (shared, _sim) = harness(
        config(SyncPolicy::Never, Preallocate::Chunk),
        FaultPlan::new(1),
    );
    let appender = Arc::new(Appender::open(Arc::clone(&shared), 0).expect("open"));

    // Standing in for the writer part way through the build, which holds this across a
    // file creation, a directory sync and the header write.
    let building = lock(&appender.spare);

    let passing = Arc::clone(&appender);
    let has_returned = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&has_returned);
    let writer = thread::spawn(move || {
        passing.prepare_spare();
        flag.store(true, Ordering::SeqCst);
    });

    thread::sleep(std::time::Duration::from_millis(50));
    assert!(
        has_returned.load(Ordering::SeqCst),
        "a writer queued behind the build instead of walking away"
    );

    drop(building);
    writer.join().expect("writer joins");
}

// a sealed segment takes the readahead hint before any reader finds it cached
#[test]
fn a_seal_hints_against_readahead() {
    let (shared, sim) = harness(
        config(SyncPolicy::Never, Preallocate::Chunk),
        FaultPlan::new(1),
    );
    let appender = Appender::open(Arc::clone(&shared), 0).expect("open");
    appender
        .append_data(key(1), vec![0x11; 400], 0, Commit::PerRecord)
        .expect("append");

    let before = sim
        .advise_traces()
        .into_iter()
        .filter(|trace| matches!(trace.advice, Advice::Random))
        .count();

    appender.seal().expect("seal");

    // The tail's own handle is what readers find in the cache after a seal, so the hint
    // has to be on it and not only on a handle a read-path open made.
    let hinted = sim
        .advise_traces()
        .into_iter()
        .filter(|trace| matches!(trace.advice, Advice::Random))
        .count();
    assert!(
        hinted > before,
        "the sealed segment was left with readahead on"
    );
}

// a segment the tail rolled off is sealed by the time a flush returns
#[test]
fn a_rolled_segment_is_sealed_once_a_flush_returns() {
    let (shared, sim) = harness(
        config(SyncPolicy::Never, Preallocate::Chunk),
        FaultPlan::new(1),
    );
    let appender = Appender::open(Arc::clone(&shared), 0).expect("open");

    // Fill the segment so the tail rolls off it without anyone asking for a seal.
    let payload = vec![0x11u8; 4 * 1024];
    let mut rolled = false;
    for byte in 0..255u8 {
        appender
            .append_data(key(byte), payload.clone(), 0, Commit::PerRecord)
            .expect("append");
        // The tail rolled once it is appending somewhere other than segment one.
        if appender.tail.active_segment() != SegmentId(1) {
            rolled = true;
            break;
        }
    }
    assert!(rolled, "the tail never rolled, so this proves nothing");

    // The write that rolled has returned, and its footer may not be down yet: the writer
    // did not wait for it.
    appender.flush().expect("flush");

    // And after the flush it is, which is the guarantee a caller still has.
    assert!(
        !shared.is_held(SegmentId(1)),
        "a flush returned with a rolled segment still unsealed"
    );
    let sealed = sim
        .advise_traces()
        .into_iter()
        .filter(|trace| matches!(trace.advice, Advice::Random))
        .count();
    assert!(sealed > 0, "no segment was sealed at all");
}

// a segment is held from the moment it is drawn until its seal has landed
#[test]
fn a_tail_holds_its_segment_until_it_is_sealed() {
    let (shared, _sim) = harness(
        config(SyncPolicy::Never, Preallocate::Chunk),
        FaultPlan::new(1),
    );
    let appender = Appender::open(Arc::clone(&shared), 0).expect("open");

    assert!(
        shared.is_held(SegmentId(1)),
        "the tail holds the segment it opened on"
    );

    // The spare is drawn before the roll needs it, and the maintenance plane has to leave
    // it alone from then on rather than from when the tail adopts it.
    appender.prepare_spare();
    assert!(
        shared.is_held(SegmentId(2)),
        "a spare is held before it is adopted"
    );

    appender
        .append_data(key(1), vec![0x11; 400], 0, Commit::PerRecord)
        .expect("append");
    appender.seal().expect("seal");

    assert!(
        !shared.is_held(SegmentId(1)),
        "a sealed segment is given up"
    );
    assert!(
        shared.is_held(SegmentId(2)),
        "the segment it rolled to is held"
    );
}

// a sealed segment stays held until the record it took has been published
#[test]
fn a_landed_record_holds_its_segment_until_it_is_published() {
    let (shared, _sim) = harness(
        config(SyncPolicy::Never, Preallocate::Chunk),
        FaultPlan::new(1),
    );
    let appender = Appender::open(Arc::clone(&shared), 0).expect("open");

    let committed = appender
        .append_data(key(1), vec![0x11; 400], 0, Commit::PerRecord)
        .expect("append");
    appender.seal().expect("seal");

    assert!(
        shared.is_held(SegmentId(1)),
        "a record on the device that nobody has published holds its segment"
    );

    drop(committed);
    assert!(
        !shared.is_held(SegmentId(1)),
        "publishing it gives the segment up"
    );
}

// sealing writes a footer that parses and starts a fresh segment
#[test]
fn seal_writes_valid_footer_and_rolls() {
    let (shared, sim) = harness(
        config(SyncPolicy::Never, Preallocate::Chunk),
        FaultPlan::new(1),
    );
    let appender = Appender::open(Arc::clone(&shared), 0).expect("open");

    appender
        .append_data(key(1), vec![0x11; 400], 0, Commit::PerRecord)
        .expect("append");
    appender
        .append_tombstone(key(2), Commit::PerRecord)
        .expect("tombstone");
    appender.seal().expect("seal");

    assert_eq!(appender.tail().active_segment(), SegmentId(2));

    let bytes = sim
        .durable_bytes(&shared.segment_path(SegmentId(1)))
        .expect("durable");
    let footer = SegmentFooter::parse(&bytes).expect("footer");
    assert_eq!(footer.entry_count(), 2);
    assert_eq!(footer.min_lsn, Lsn(1));
    assert_eq!(footer.max_lsn, Lsn(2));
}

// a chunked volume reserves its next chunk before the head reaches the last
#[test]
fn reservation_steps_ahead_of_the_head() {
    let (shared, _sim) = harness(
        config(SyncPolicy::Never, Preallocate::Chunk),
        FaultPlan::new(1),
    );
    let appender = Appender::open(Arc::clone(&shared), 0).expect("open");
    let chunk = shared.config.alloc_chunk.to_bytes();

    let mut byte = 0u8;
    while appender.tail().committed_len() < chunk {
        byte = byte.wrapping_add(1);
        appender
            .append_data(key(byte), vec![byte; 500], 0, Commit::PerRecord)
            .expect("append");
    }

    let path = shared.segment_path(SegmentId(1));
    let name = path
        .file_name()
        .expect("name")
        .to_string_lossy()
        .into_owned();
    let entries = shared.driver.list(Path::new(REEL_DIR)).expect("list");
    assert!(
        entry_len(&entries, &name) > chunk,
        "the write head passed the first chunk with nothing reserved behind it"
    );
}

// the segment a tail rolls to exists before the roll asks for it
#[test]
fn spare_is_drawn_before_the_roll() {
    let (shared, _sim) = harness(
        config(SyncPolicy::Never, Preallocate::Chunk),
        FaultPlan::new(1),
    );
    let appender = Appender::open(Arc::clone(&shared), 0).expect("open");
    let target = shared.config.segment_bytes.to_bytes();
    let margin = (target / 16).min(WRITEBACK_CHUNK);

    let mut byte = 0u8;
    while appender.tail().committed_len() + margin < target {
        byte = byte.wrapping_add(1);
        appender
            .append_data(key(byte), vec![byte; 500], 0, Commit::PerRecord)
            .expect("append");
    }
    appender
        .append_data(key(1), vec![0x01; 500], 0, Commit::PerRecord)
        .expect("the record that crosses");

    assert_eq!(
        appender.tail().active_segment(),
        SegmentId(1),
        "the tail has not rolled"
    );
    let next = shared.segment_path(SegmentId(2));
    let name = next
        .file_name()
        .expect("name")
        .to_string_lossy()
        .into_owned();
    let entries = shared.driver.list(Path::new(REEL_DIR)).expect("list");
    assert!(
        entries.iter().any(|entry| entry.name == name),
        "the next segment was not drawn"
    );
}

// chunk preallocation slack is closed at seal so the footer ends the file
#[test]
fn seal_bridges_preallocation_slack() {
    let (shared, sim) = harness(
        config(SyncPolicy::Never, Preallocate::Chunk),
        FaultPlan::new(1),
    );
    let appender = Appender::open(Arc::clone(&shared), 0).expect("open");

    appender
        .append_data(key(1), vec![0x11; 128], 0, Commit::PerRecord)
        .expect("append");
    appender.seal().expect("seal");

    let bytes = sim
        .durable_bytes(&shared.segment_path(SegmentId(1)))
        .expect("durable");
    let footer = SegmentFooter::parse(&bytes).expect("footer parses at the file end");
    assert_eq!(footer.entry_count(), 1);
}

// a drain that never landed leaves no footer entry at the offset it framed
#[test]
fn failed_drain_leaves_no_footer_entry() {
    let plan = FaultPlan::new(1).with_fault(4, FaultKind::EnospcAppend);
    let (shared, sim) = harness(config(SyncPolicy::Never, Preallocate::Chunk), plan);
    let appender = Appender::open(Arc::clone(&shared), 0).expect("open");

    let refused = appender.append_data(key(1), vec![0x11; 300], 0, Commit::PerRecord);
    assert!(refused.is_err(), "the out of space drain is refused");
    appender
        .append_data(key(2), vec![0x22; 300], 0, Commit::PerRecord)
        .expect("the next drain lands");
    appender.seal().expect("seal");

    let bytes = sim
        .durable_bytes(&shared.segment_path(SegmentId(1)))
        .expect("durable");
    let footer = SegmentFooter::parse(&bytes).expect("footer");

    assert_eq!(
        footer.entry_count(),
        1,
        "the refused drain left a phantom entry"
    );
    let only = footer.entries().next().expect("entry").expect("row");
    assert_eq!(only.key, key(2));
}

// a failed cadence sync gives up on the segment and rolls off it
#[test]
fn failed_sync_rolls_off_the_segment() {
    // The open, the directory sync, the space reservation, the segment header write and
    // the record write all come first, so the tail's first sync is the sixth op.
    let plan = FaultPlan::new(1).with_fault(5, FaultKind::SyncError);
    let (shared, _sim) = harness(config(SyncPolicy::EveryPut, Preallocate::Chunk), plan);
    let appender = Appender::open(Arc::clone(&shared), 0).expect("open");

    let outcome = appender.append_data(key(1), vec![0x11; 300], 0, Commit::PerRecord);
    assert!(outcome.is_err());
    assert_eq!(appender.tail().active_segment(), SegmentId(2));
}

// a segment given up on unsealed never reads as settled
#[test]
fn a_doomed_segment_never_settles() {
    let plan = FaultPlan::new(1).with_fault(5, FaultKind::SyncError);
    let (shared, _sim) = harness(config(SyncPolicy::EveryPut, Preallocate::Chunk), plan);
    let appender = Appender::open(Arc::clone(&shared), 0).expect("open");

    let outcome = appender.append_data(key(1), vec![0x11; 300], 0, Commit::PerRecord);
    assert!(outcome.is_err());
    assert!(
        !shared.is_held(SegmentId(1)),
        "the roll left the segment held"
    );
    assert!(
        !shared.is_settled(SegmentId(1)),
        "an unsealed segment read as settled"
    );
}

// records appended concurrently all commit and read back where they landed
#[test]
fn concurrent_appends_all_commit() {
    let (shared, _sim) = harness(
        config(SyncPolicy::Never, Preallocate::Chunk),
        FaultPlan::new(1),
    );
    let appender = Arc::new(Appender::open(Arc::clone(&shared), 0).expect("open"));

    let writers = 8u8;
    let barrier = Arc::new(Barrier::new(writers as usize));
    let mut handles = Vec::new();
    for byte in 0..writers {
        let appender = Arc::clone(&appender);
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            barrier.wait();
            appender
                .append_data(key(byte + 1), vec![byte + 1; 250], 0, Commit::PerRecord)
                .expect("append")
        }));
    }
    let mut committed = Vec::new();
    for handle in handles {
        committed.push(handle.join().expect("join"));
    }
    assert_eq!(committed.len(), writers as usize);

    let bytes = read_segment(&shared, &shared.segment_path(SegmentId(1)));
    for record in &committed {
        let start = record.loc.offset as usize;
        let header = RecordHeader::unpack(&bytes[start..]).expect("header");
        let payload = &bytes[start + HEADER_LEN..start + HEADER_LEN + header.length as usize];
        assert!(header.verify(payload));
    }
}

// records land intact while another writer's sync is out at the device
#[test]
fn appends_land_during_a_sync() {
    let (shared, _sim) = harness(
        config(SyncPolicy::EveryPut, Preallocate::Chunk),
        FaultPlan::new(1),
    );
    let appender = Arc::new(Appender::open(Arc::clone(&shared), 0).expect("open"));

    let writers = 8u8;
    let barrier = Arc::new(Barrier::new(writers as usize));
    let mut handles = Vec::new();
    for byte in 0..writers {
        let appender = Arc::clone(&appender);
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            barrier.wait();
            appender
                .append_data(key(byte + 1), vec![byte + 1; 250], 0, Commit::PerRecord)
                .expect("append")
        }));
    }
    let mut committed = Vec::new();
    for handle in handles {
        committed.push(handle.join().expect("join"));
    }

    let bytes = read_segment(&shared, &shared.segment_path(SegmentId(1)));
    let records = walk(&bytes, appender.tail().committed_len());
    for record in &committed {
        let start = record.loc.offset as usize;
        let header = RecordHeader::unpack(&bytes[start..]).expect("header");
        let payload = &bytes[start + HEADER_LEN..start + HEADER_LEN + header.length as usize];
        assert!(header.verify(payload));
    }
    assert!(
        !records.is_empty(),
        "the walk crossed every record the writers left"
    );
}

// writers crossing one threshold together share a flush instead of each buying one
#[test]
fn writers_share_one_flush() {
    let mut settings = config(
        SyncPolicy::Bytes(ByteCount::from_bytes(4096)),
        Preallocate::Chunk,
    );
    settings.segment_bytes = ByteCount::mb(8);
    let (shared, sim) = harness(settings, FaultPlan::new(1));
    let appender = Arc::new(Appender::open(Arc::clone(&shared), 0).expect("open"));

    let writers = 16u8;
    let rounds = 8;
    let barrier = Arc::new(Barrier::new(writers as usize));
    let mut handles = Vec::new();
    for byte in 0..writers {
        let appender = Arc::clone(&appender);
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            barrier.wait();
            for _ in 0..rounds {
                appender
                    .append_data(key(byte + 1), vec![byte + 1; 1000], 0, Commit::PerRecord)
                    .expect("append");
            }
        }));
    }
    for handle in handles {
        handle.join().expect("join");
    }

    // Every record is a quarter of the threshold, so the bytes written owe this many
    // flushes, where a writer asking on its own would pay a multiple of it.
    let written = u64::from(writers) * rounds * 1000;
    let owed = written / 4096;
    assert!(
        sim.sync_count() <= owed * 2,
        "{} flushes for {owed} thresholds worth of writes",
        sim.sync_count()
    );
}

// writers keep committing while flushes and rolls interleave
#[test]
fn flush_survives_a_roll_underneath_it() {
    let mut settings = config(
        SyncPolicy::Bytes(ByteCount::from_bytes(1024)),
        Preallocate::Chunk,
    );
    settings.segment_bytes = ByteCount::from_bytes(ALIGN * 16);
    let (shared, _sim) = harness(settings, FaultPlan::new(1));
    let appender = Arc::new(Appender::open(Arc::clone(&shared), 0).expect("open"));

    let writers = 8u8;
    let per_writer = 64;
    let barrier = Arc::new(Barrier::new(writers as usize));
    let mut handles = Vec::new();
    for writer in 0..writers {
        let appender = Arc::clone(&appender);
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            barrier.wait();
            // Every record carries its writer's byte, so a record read back where it was
            // reported is checked against what that writer wrote.
            let byte = writer + 1;
            let mut committed = Vec::new();
            for _ in 0..per_writer {
                committed.push(
                    appender
                        .append_data(key(byte), vec![byte; 250], 0, Commit::PerRecord)
                        .expect("append"),
                );
            }
            committed
        }));
    }
    let mut committed = Vec::new();
    for handle in handles {
        committed.extend(handle.join().expect("join"));
    }

    assert_eq!(committed.len(), writers as usize * per_writer);
    assert!(
        appender.tail().active_segment() > SegmentId(1),
        "the tail rolled, so flushes and rolls really did interleave"
    );
    for record in &committed {
        let bytes = read_segment(&shared, &shared.segment_path(record.loc.segment));
        let start = record.loc.offset as usize;
        let header = RecordHeader::unpack(&bytes[start..]).expect("header");
        let payload = &bytes[start + HEADER_LEN..start + HEADER_LEN + header.length as usize];
        assert!(
            header.verify(payload),
            "every record verifies where it landed"
        );
    }
}

// a one-record budget serializes writers and returns the gauge to zero
#[test]
fn budget_backpressure_serializes() {
    let (shared, _sim) = harness_capped(
        config(SyncPolicy::Never, Preallocate::Chunk),
        FaultPlan::new(1),
        InflightBudget::new(ByteCount::from_bytes(HEADER_LEN as u64 + 250)),
    );
    let appender = Arc::new(Appender::open(Arc::clone(&shared), 0).expect("open"));

    let mut handles = Vec::new();
    for byte in 0..6u8 {
        let appender = Arc::clone(&appender);
        handles.push(thread::spawn(move || {
            appender
                .append_data(key(byte + 1), vec![byte + 1; 250], 0, Commit::PerRecord)
                .expect("append")
        }));
    }
    for handle in handles {
        handle.join().expect("join");
    }
    assert_eq!(shared.budget.queued_bytes(), ByteCount::from_bytes(0));
}

// a volume never asks for its own pages back, however far the write head has run
#[test]
fn a_write_head_asks_for_nothing() {
    let mut settings = config(SyncPolicy::Bytes(ByteCount::gb(1)), Preallocate::Chunk);
    settings.segment_bytes = ByteCount::mb(256);
    let (shared, sim) = harness(settings, FaultPlan::new(1));
    let appender = Appender::open(Arc::clone(&shared), 0).expect("open");

    let record = (8 * 1024 * 1024) as usize;
    for byte in 1..=16u8 {
        appender
            .append_data(key(byte), vec![byte; record], 0, Commit::PerRecord)
            .expect("append");
    }

    assert!(
        sim.advise_traces().is_empty(),
        "the write head said something about its pages"
    );
}
