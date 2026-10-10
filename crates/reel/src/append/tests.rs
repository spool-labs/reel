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
use crate::config::{IoBackend, ReelConfig, DEFAULT_FD_CACHE};
use crate::format::column::{Codec, ColumnId, ColumnSet, ColumnSpec, KeyWidth};
use crate::format::footer::SegmentFooter;
use crate::format::journal::{read_groups, rows_region, JournalRow};
use crate::format::loc::SegmentId;
use crate::format::record::{check_keyless, KeylessRead, KEYLESS_MAX, KEYLESS_PREFIX};
use crate::format::segment_header::{SegmentHeader, SEGMENT_HEADER_SPAN};
use crate::io::fault::{FaultKind, FaultPlan};
use crate::io::op::{Advice, SegmentEntry};
use crate::io::sim_backend::SimIo;
use crate::reel::segment::{FdCache, IoDriver};
use crate::sync::tension::block_on;

/// A sync on every write, for tests that need each one durable
const EVERY_WRITE: SyncPolicy = SyncPolicy::Bytes(ByteCount::from_bytes(0));

const REEL_DIR: &str = "/bulk";
const RECORDS: ColumnId = ColumnId(1);
const KEY_WIDTH: usize = 34;
const SEG_HEADER_SPAN: u64 = HEADER_LEN as u64 + SEGMENT_HEADER_SPAN as u64;

/// One fixed-key column for the append path tests
const COLUMNS: ColumnSet = &[ColumnSpec {
    id: RECORDS,
    name: "records",
    key_width: KeyWidth::Fixed(KEY_WIDTH as u16),
    shard_bytes: 2,
    purge_mark: None,
    codec: Codec::None,
}];

fn config(sync: SyncPolicy) -> ReelConfig {
    ReelConfig {
        segment_bytes: ByteCount::mb(1),
        sync,
        ..ReelConfig::default()
    }
}

fn harness(config: ReelConfig, plan: FaultPlan) -> (Arc<ReelShared>, SimIo) {
    harness_capped(config, plan, InflightBudget::default())
}

/// The same harness with an admission ceiling, for the backpressure tests
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

// pending rows keep every group in order across flushes, and a flush writes none of them twice
#[test]
fn pending_rows_survive_flushes() {
    let (shared, _sim) = harness(config(SyncPolicy::Never), FaultPlan::new(1));
    let file = shared
        .driver
        .open(&shared.segment_path(SegmentId(9)), true)
        .expect("open");
    let rows_at = 4096;
    let journal = Journal::create(&shared.driver, file, rows_at, 1 << 20, false);
    let row = |byte: u8| JournalRow {
        key: key(byte),
        lsn: crate::format::lsn::Lsn(u64::from(byte)),
        offset: 100 * u32::from(byte),
        len: 40,
        flags: crate::format::record::Flags::DATA,
        range_end: None,
    };
    let groups = vec![vec![row(1)], vec![row(2), row(3)], vec![row(4)]];
    journal.push(&groups[0]);
    journal.push(&groups[1]);
    journal.write_pending().expect("flush");
    journal.push(&groups[2]);
    journal.write_pending().expect("flush");

    let bytes = shared
        .driver
        .pread(file, rows_at, journal.len())
        .expect("read");
    assert_eq!(read_groups(&bytes).0, groups);
}

// a short journal write keeps its rows, and the next write lands them whole over the torn part
#[test]
fn a_short_journal_write_lands_whole_on_retry() {
    for whole_blocks in [false, true] {
        let (shared, sim) = harness(config(SyncPolicy::Never), FaultPlan::new(1));
        let file = shared
            .driver
            .open(&shared.segment_path(SegmentId(9)), true)
            .expect("open");
        let rows_at = 4096;
        let journal = Journal::create(&shared.driver, file, rows_at, 1 << 20, whole_blocks);
        let row = |byte: u8| JournalRow {
            key: key(byte),
            lsn: crate::format::lsn::Lsn(u64::from(byte)),
            offset: 100 * u32::from(byte),
            len: 40,
            flags: crate::format::record::Flags::DATA,
            range_end: None,
        };
        let groups = vec![vec![row(1), row(2)], vec![row(3)]];
        journal.push(&groups[0]);
        sim.arm_next_ops(1, FaultKind::ShortWrite { written_bytes: 10 });
        assert!(
            journal.write_pending().is_err(),
            "the short write is refused"
        );
        journal.push(&groups[1]);
        journal.write_pending().expect("the retry lands");

        let bytes = shared
            .driver
            .pread(file, rows_at, journal.len())
            .expect("read");
        let (read, valid) = read_groups(&bytes);
        assert_eq!(read, groups, "whole blocks: {whole_blocks}");
        let padded = match whole_blocks {
            true => (valid as u64).next_multiple_of(BLOCK),
            false => valid as u64,
        };
        assert_eq!(journal.len(), padded, "the retry counted its padding twice");
    }
}

// a failed pace write keeps its rows, so the flush after it makes them durable
#[test]
fn a_failed_journal_write_keeps_its_rows() {
    let (shared, sim) = harness(config(SyncPolicy::Never), FaultPlan::new(1));
    let appender = Appender::open(Arc::clone(&shared), 0, None).expect("open");
    appender
        .append_data(key(1), vec![0x11; 500], 0, Commit::PerRecord)
        .expect("append 1");
    sim.arm_next_ops(1, FaultKind::EnospcAppend);
    let fired = sim.fault_reach().0;
    read(&appender.active).journal.try_write_pending();
    assert_eq!(sim.fault_reach().0, fired + 1, "the pace write failed");
    appender
        .append_data(key(2), vec![0x22; 500], 0, Commit::PerRecord)
        .expect("append 2");
    appender.flush().expect("flush");

    let bytes = sim
        .durable_bytes(&shared.segment_path(SegmentId(1)))
        .expect("durable");
    let rows = rows_region(&bytes).map_or(&[][..], |(_, rows)| rows);
    let keys: Vec<RecordKey> = read_groups(rows)
        .0
        .into_iter()
        .flatten()
        .map(|row| row.key)
        .collect();
    assert_eq!(keys, vec![key(1), key(2)], "a flushed record lost its row");
}

/// One flush syncs the segment file once, which covers the rows too
const SYNCS_PER_FLUSH: u64 = 1;

fn key(byte: u8) -> RecordKey {
    RecordKey::from_bytes(RECORDS, &[byte; KEY_WIDTH]).expect("key")
}

/// Poll a future once with a waker that does nothing
fn poll_once<Awaited: Future>(future: Pin<&mut Awaited>) -> Poll<Awaited::Output> {
    future.poll(&mut Context::from_waker(Waker::noop()))
}

/// How many bytes a record with this payload takes in this column
fn framed(payload_len: usize) -> u64 {
    crate::index::entry::span_of(KEY_WIDTH as u16, payload_len as u32)
}

/// Flush, then read back the row groups the open segment journaled
fn journaled(shared: &ReelShared, appender: &Appender, segment: SegmentId) -> Vec<Vec<JournalRow>> {
    appender.flush().expect("flush");
    let bytes = read_segment(shared, &shared.segment_path(segment));
    read_groups(rows_region(&bytes).map_or(&[][..], |(_, rows)| rows)).0
}

/// Whether the record at a location checks out against the key its payload's byte gives
fn lands_intact(shared: &ReelShared, loc: Loc) -> bool {
    let bytes = read_segment(shared, &shared.segment_path(loc.segment));
    let header = RecordHeader::unpack(&bytes).expect("segment header");
    let payload = &bytes[HEADER_LEN..HEADER_LEN + header.length as usize];
    let layout = SegmentHeader::unpack(payload)
        .expect("segment header payload")
        .layout;
    let check = layout
        .keyless_key(loc.len)
        .expect("a small record lies keyless");
    let start = loc.offset as usize;
    let (prefix, payload) =
        bytes[start..start + KEYLESS_PREFIX + loc.len as usize].split_at(KEYLESS_PREFIX);
    let key = key(payload[0]);
    matches!(
        check_keyless(prefix, payload, key.as_ref(), Flags::DATA, &check),
        KeylessRead::Intact(_)
    )
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

// opening a tail writes the segment header as record zero and nothing else
#[test]
fn open_writes_segment_header() {
    let (shared, _sim) = harness(config(EVERY_WRITE), FaultPlan::new(1));
    let appender = Appender::open(Arc::clone(&shared), 0, None).expect("open");

    assert_eq!(appender.tail().active_segment(), SegmentId(1));
    assert_eq!(appender.tail().committed_len(), SEG_HEADER_SPAN);

    let bytes = read_segment(&shared, &shared.segment_path(SegmentId(1)));
    let header = RecordHeader::unpack(&bytes).expect("header");
    assert!(header.flags.is_segment_header());
    assert_eq!(header.span(), SEG_HEADER_SPAN);
    assert!(
        journaled(&shared, &appender, SegmentId(1)).is_empty(),
        "the header lists no row"
    );
}

// a whole-block volume closes the header drain with zeros to the boundary
#[test]
fn whole_block_open_fills_to_boundary() {
    let mut settings = config(EVERY_WRITE);
    settings.io_backend = IoBackend::UringDirect;
    let (shared, _sim) = harness(settings, FaultPlan::new(1));
    let appender = Appender::open(Arc::clone(&shared), 0, None).expect("open");

    assert_eq!(appender.tail().committed_len(), ALIGN);

    let bytes = read_segment(&shared, &shared.segment_path(SegmentId(1)));
    let header = RecordHeader::unpack(&bytes).expect("header");
    assert!(header.flags.is_segment_header());
    assert!(bytes[header.span() as usize..ALIGN as usize]
        .iter()
        .all(|byte| *byte == 0));
}

// records land back to back after the segment header, each where it reserved
#[test]
fn records_land_contiguously() {
    let (shared, _sim) = harness(config(SyncPolicy::Never), FaultPlan::new(1));
    let appender = Appender::open(Arc::clone(&shared), 0, None).expect("open");

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

    let data: Vec<JournalRow> = journaled(&shared, &appender, SegmentId(1))
        .into_iter()
        .flatten()
        .collect();
    assert_eq!(data.len(), 3);
    assert_eq!(u64::from(data[0].offset), SEG_HEADER_SPAN);
    assert_eq!(u64::from(data[1].offset), SEG_HEADER_SPAN + framed(100));
    assert_eq!(
        u64::from(data[2].offset),
        SEG_HEADER_SPAN + framed(100) + framed(200)
    );
}

// every drain of a whole-block volume leaves the write head on a boundary
#[test]
fn every_whole_block_drain_is_aligned() {
    let mut settings = config(SyncPolicy::Never);
    settings.io_backend = IoBackend::UringDirect;
    let (shared, _sim) = harness(settings, FaultPlan::new(1));
    let appender = Appender::open(Arc::clone(&shared), 0, None).expect("open");

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

// a sync leaves the write head exactly where the records left it
#[test]
fn a_sync_adds_no_bytes() {
    let (shared, sim) = harness(config(EVERY_WRITE), FaultPlan::new(1));
    let appender = Appender::open(Arc::clone(&shared), 0, None).expect("open");

    appender
        .append_data(key(1), vec![0x11; 500], 0, Commit::PerRecord)
        .expect("first");

    // The flush writes no bookkeeping record
    let record_end = SEG_HEADER_SPAN + framed(500);
    assert_eq!(appender.tail().committed_len(), record_end);
    assert!(sim.sync_count() > 0, "the put reached the device");

    assert_eq!(
        journaled(&shared, &appender, SegmentId(1)).concat().len(),
        1,
        "the record and nothing else"
    );
}

// a flush makes what has settled durable without adding a record for it
#[test]
fn flush_syncs_what_settled() {
    let (shared, sim) = harness(config(SyncPolicy::Never), FaultPlan::new(1));
    let appender = Appender::open(Arc::clone(&shared), 0, None).expect("open");

    appender
        .append_data(key(1), vec![0x11; 500], 0, Commit::PerRecord)
        .expect("append");
    let record_end = appender.tail().committed_len();
    let before = sim.sync_count();

    appender.flush().expect("flush");

    assert!(sim.sync_count() > before, "the flush reached the device");
    assert_eq!(
        appender.tail().committed_len(),
        record_end,
        "the flush wrote no record of its own"
    );
}

// a tail with no sync owed answers the awaitable wait without waiting
#[test]
fn nothing_owed_settles_at_once() {
    let (shared, sim) = harness(config(SyncPolicy::Never), FaultPlan::new(1));
    let appender = Appender::open(Arc::clone(&shared), 0, None).expect("open");

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

// the awaitable wait hands the turn back to the caller and leaves the device alone
#[test]
fn an_owed_sync_hands_back_the_turn() {
    let (shared, sim) = harness(config(EVERY_WRITE), FaultPlan::new(1));
    let appender = Appender::open(Arc::clone(&shared), 0, None).expect("open");

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
    let (shared, sim) = harness(config(EVERY_WRITE), FaultPlan::new(1));
    let appender = Appender::open(Arc::clone(&shared), 0, None).expect("open");

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
    assert_eq!(
        sim.sync_count(),
        before + SYNCS_PER_FLUSH,
        "one flush's syncs"
    );
}

// a turn nobody takes goes back, so the writer behind it is not left waiting
#[test]
fn a_dropped_turn_frees_the_device() {
    let (shared, _sim) = harness(config(EVERY_WRITE), FaultPlan::new(1));
    let appender = Appender::open(Arc::clone(&shared), 0, None).expect("open");

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
    let (shared, sim) = harness(config(EVERY_WRITE), FaultPlan::new(1));
    let appender = Appender::open(Arc::clone(&shared), 0, None).expect("open");

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
    let (shared, sim) = harness(config(EVERY_WRITE), FaultPlan::new(1));
    let appender = Appender::open(Arc::clone(&shared), 0, None).expect("open");

    appender
        .append_data(key(1), vec![0x11; 500], 0, Commit::Batched)
        .expect("append");
    let before = sim.sync_count();

    let mut first = Box::pin(appender.sync_if_owed_wait());
    // This poll draws the turn and hands it over, and its answer races the sealer
    let handed = poll_once(first.as_mut());
    block_on(appender.sync_if_owed_wait()).expect("second writer");
    match handed {
        Poll::Ready(answered) => answered.expect("first writer"),
        Poll::Pending => block_on(first).expect("first writer"),
    }

    assert_eq!(
        sim.sync_count(),
        before + SYNCS_PER_FLUSH,
        "one flush's syncs"
    );
}

// a caller that walks away from a forwarded flush leaves the turn where it is
#[test]
fn a_dropped_wait_keeps_the_flush() {
    let (shared, sim) = harness(config(EVERY_WRITE), FaultPlan::new(1));
    let appender = Appender::open(Arc::clone(&shared), 0, None).expect("open");

    appender
        .append_data(key(1), vec![0x11; 500], 0, Commit::Batched)
        .expect("append");
    let before = sim.sync_count();

    {
        let mut walked = Box::pin(appender.sync_if_owed_wait());
        let _ = poll_once(walked.as_mut());
    }

    // The sealer holds the turn now, so this waits for it
    appender.sync_if_owed().expect("blocking sync");
    assert_eq!(
        sim.sync_count(),
        before + SYNCS_PER_FLUSH,
        "the abandoned flush is the one that ran"
    );
}

// an awaited append waits for admission without holding a thread
#[test]
fn an_awaited_append_waits_for_room() {
    let (shared, _sim) = harness_capped(
        config(SyncPolicy::Never),
        FaultPlan::new(1),
        InflightBudget::new(ByteCount::from_bytes(4096)),
    );
    let appender = Appender::open(Arc::clone(&shared), 0, None).expect("open");
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
        config(EVERY_WRITE),
        FaultPlan::new(1),
        InflightBudget::new(ByteCount::from_bytes(4096)),
    );
    let appender = Appender::open(Arc::clone(&shared), 0, None).expect("open");

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

/// A batch of three records of the same width, for the framing tests
fn batch_of(payload_len: usize) -> Vec<BatchRecord> {
    (1..=3u8)
        .map(|byte| BatchRecord {
            key: key(byte),
            write: BatchWrite::Put(vec![byte; payload_len], 0),
        })
        .collect()
}

// a small record lies keyless, and a record past the ceiling keeps its header and key
#[test]
fn a_small_record_lies_keyless() {
    let (shared, _sim) = harness(config(SyncPolicy::Never), FaultPlan::new(1));
    let appender = Appender::open(Arc::clone(&shared), 0, None).expect("open");

    let small = appender
        .append_data(key(0x11), vec![0x11; 100], 0, Commit::PerRecord)
        .expect("small");
    let wide = KEYLESS_MAX as usize + 1;
    let large = appender
        .append_data(key(0x22), vec![0x22; wide], 0, Commit::PerRecord)
        .expect("large");

    assert!(lands_intact(&shared, small.loc));
    assert_eq!(
        u64::from(large.loc.offset),
        u64::from(small.loc.offset) + KEYLESS_PREFIX as u64 + 100,
        "the small record took its prefix and payload and nothing else"
    );
    let bytes = read_segment(&shared, &shared.segment_path(SegmentId(1)));
    let at = large.loc.offset as usize;
    let header = RecordHeader::unpack(&bytes[at..]).expect("a large record keeps its header");
    assert_eq!(header.key, key(0x22));
    let payload_at = at + header.prefix_len() as usize;
    assert!(header.verify(&bytes[payload_at..payload_at + wide]));
}

// a batch lands back to back and journals its rows as one group
#[test]
fn a_batch_journals_as_one_group() {
    let (shared, _sim) = harness(config(SyncPolicy::Never), FaultPlan::new(1));
    let appender = Appender::open(Arc::clone(&shared), 0, None).expect("open");

    appender.append_batch(batch_of(300)).expect("batch");

    assert_eq!(
        appender.tail().committed_len(),
        SEG_HEADER_SPAN + framed(300) * 3
    );
    let groups = journaled(&shared, &appender, SegmentId(1));
    assert_eq!(groups.len(), 1, "the batch journaled as one group");
    let offsets: Vec<u64> = groups[0].iter().map(|row| u64::from(row.offset)).collect();
    assert_eq!(
        offsets,
        vec![
            SEG_HEADER_SPAN,
            SEG_HEADER_SPAN + framed(300),
            SEG_HEADER_SPAN + framed(300) * 2
        ]
    );
}

// a whole-block volume fills a batch out to the next boundary with zeros behind the run
#[test]
fn a_whole_block_batch_fills_behind_its_run() {
    let mut settings = config(SyncPolicy::Never);
    settings.io_backend = IoBackend::UringDirect;
    let (shared, _sim) = harness(settings, FaultPlan::new(1));
    let appender = Appender::open(Arc::clone(&shared), 0, None).expect("open");

    appender.append_batch(batch_of(300)).expect("batch");

    let committed = appender.tail().committed_len();
    assert_eq!(
        committed % ALIGN,
        0,
        "the batch left the head off a boundary"
    );
    let run_end = ALIGN + framed(300) * 3;
    let bytes = read_segment(&shared, &shared.segment_path(SegmentId(1)));
    assert!(bytes[run_end as usize..committed as usize]
        .iter()
        .all(|byte| *byte == 0));
}

// a batch too wide for the room left rolls whole into the next segment
#[test]
fn a_batch_never_spans_segments() {
    let mut settings = config(SyncPolicy::Never);
    settings.segment_bytes = ByteCount::from_bytes(64 * 1024);
    let (shared, _sim) = harness(settings, FaultPlan::new(1));
    let appender = Appender::open(Arc::clone(&shared), 0, None).expect("open");

    let payload = 4_000;
    let batch_span = framed(payload) * 3;
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

    let offsets: Vec<u64> = committed
        .iter()
        .map(|record| u64::from(record.loc.offset))
        .collect();
    assert_eq!(
        offsets,
        vec![
            offsets[0],
            offsets[0] + framed(payload),
            offsets[0] + framed(payload) * 2
        ],
        "the run lands back to back",
    );
}

// a writer that finds the spare being drawn walks away without queueing
#[test]
fn drawing_a_spare_never_queues_a_writer() {
    let (shared, _sim) = harness(config(SyncPolicy::Never), FaultPlan::new(1));
    let appender = Arc::new(Appender::open(Arc::clone(&shared), 0, None).expect("open"));

    // Stands in for a writer part way through building the spare
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
    let (shared, sim) = harness(config(SyncPolicy::Never), FaultPlan::new(1));
    let appender = Appender::open(Arc::clone(&shared), 0, None).expect("open");
    appender
        .append_data(key(1), vec![0x11; 400], 0, Commit::PerRecord)
        .expect("append");

    let before = sim
        .advise_traces()
        .into_iter()
        .filter(|trace| matches!(trace.advice, Advice::Random))
        .count();

    appender.seal().expect("seal");

    // Readers find the tail's own handle in the cache after a seal, so it needs the hint
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
    let (shared, sim) = harness(config(SyncPolicy::Never), FaultPlan::new(1));
    let appender = Appender::open(Arc::clone(&shared), 0, None).expect("open");

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

    // The rolling write has returned without waiting for its footer
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
    let (shared, _sim) = harness(config(SyncPolicy::Never), FaultPlan::new(1));
    let appender = Appender::open(Arc::clone(&shared), 0, None).expect("open");

    assert!(
        shared.is_held(SegmentId(1)),
        "the tail holds the segment it opened on"
    );

    // The spare is held from when it is drawn, before the tail adopts it
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
    let (shared, _sim) = harness(config(SyncPolicy::Never), FaultPlan::new(1));
    let appender = Appender::open(Arc::clone(&shared), 0, None).expect("open");

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
    let (shared, sim) = harness(config(SyncPolicy::Never), FaultPlan::new(1));
    let appender = Appender::open(Arc::clone(&shared), 0, None).expect("open");

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

// a tail lays zeros down ahead of its write head
#[test]
fn reservation_steps_ahead_of_the_head() {
    let (shared, _sim) = harness(config(SyncPolicy::Never), FaultPlan::new(1));
    let appender = Appender::open(Arc::clone(&shared), 0, None).expect("open");
    let head = ALIGN * 4;

    let mut byte = 0u8;
    while appender.tail().committed_len() < head {
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
        entry_len(&entries, &name) > head,
        "the write head passed the end of the zeros laid ahead of it"
    );
}

// the segment a tail rolls to exists before the roll asks for it
#[test]
fn spare_is_drawn_before_the_roll() {
    let (shared, _sim) = harness(config(SyncPolicy::Never), FaultPlan::new(1));
    let appender = Appender::open(Arc::clone(&shared), 0, None).expect("open");
    let target = shared.config.segment_bytes.to_bytes();
    let margin = (target / 16).min(WRITEBACK_CHUNK);

    let mut byte = 0u8;
    while appender.tail().committed_len() + margin < read(&appender.active).room(target) {
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

// a drain that never landed leaves no footer entry at the offset it framed
#[test]
fn failed_drain_leaves_no_footer_entry() {
    // Op six is the record's write, after both opens, the dir sync, the reservation and the header
    let plan = FaultPlan::new(1).with_fault(5, FaultKind::EnospcAppend);
    let (shared, sim) = harness(config(SyncPolicy::Never), plan);
    let appender = Appender::open(Arc::clone(&shared), 0, None).expect("open");

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
    // Op eight is the first sync, after both opens, the dir sync, the reservation and three writes
    let plan = FaultPlan::new(1).with_fault(7, FaultKind::SyncError);
    let (shared, _sim) = harness(config(EVERY_WRITE), plan);
    let appender = Appender::open(Arc::clone(&shared), 0, None).expect("open");

    let outcome = appender.append_data(key(1), vec![0x11; 300], 0, Commit::PerRecord);
    assert!(outcome.is_err());
    assert_eq!(appender.tail().active_segment(), SegmentId(2));
}

// a segment given up on unsealed never reads as settled
#[test]
fn a_doomed_segment_never_settles() {
    let plan = FaultPlan::new(1).with_fault(7, FaultKind::SyncError);
    let (shared, _sim) = harness(config(EVERY_WRITE), plan);
    let appender = Appender::open(Arc::clone(&shared), 0, None).expect("open");

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
    let (shared, _sim) = harness(config(SyncPolicy::Never), FaultPlan::new(1));
    let appender = Arc::new(Appender::open(Arc::clone(&shared), 0, None).expect("open"));

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

    for record in &committed {
        assert!(lands_intact(&shared, record.loc));
    }
}

// records land intact while another writer's sync is out at the device
#[test]
fn appends_land_during_a_sync() {
    let (shared, _sim) = harness(config(EVERY_WRITE), FaultPlan::new(1));
    let appender = Arc::new(Appender::open(Arc::clone(&shared), 0, None).expect("open"));

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

    for record in &committed {
        assert!(lands_intact(&shared, record.loc));
    }
}

// writers crossing one threshold together share one flush
#[test]
fn writers_share_one_flush() {
    let mut settings = config(SyncPolicy::Bytes(ByteCount::from_bytes(4096)));
    settings.segment_bytes = ByteCount::mb(8);
    let (shared, sim) = harness(settings, FaultPlan::new(1));
    let appender = Arc::new(Appender::open(Arc::clone(&shared), 0, None).expect("open"));

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

    // Each record is a quarter of the threshold, so the bytes written owe this many flushes
    let written = u64::from(writers) * rounds * 1000;
    let owed = written / 4096;
    assert!(
        sim.sync_count() <= owed * 2 * SYNCS_PER_FLUSH,
        "{} syncs for {owed} thresholds worth of writes",
        sim.sync_count()
    );
}

// writers keep committing while flushes and rolls interleave
#[test]
fn flush_survives_a_roll_underneath_it() {
    let mut settings = config(SyncPolicy::Bytes(ByteCount::from_bytes(1024)));
    settings.segment_bytes = ByteCount::from_bytes(ALIGN * 16);
    let (shared, _sim) = harness(settings, FaultPlan::new(1));
    let appender = Arc::new(Appender::open(Arc::clone(&shared), 0, None).expect("open"));

    let writers = 8u8;
    let per_writer = 64;
    let barrier = Arc::new(Barrier::new(writers as usize));
    let mut handles = Vec::new();
    for writer in 0..writers {
        let appender = Arc::clone(&appender);
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            barrier.wait();
            // Each record holds its writer's byte, so a read back is checked against that writer
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
        assert!(
            lands_intact(&shared, record.loc),
            "every record verifies where it landed"
        );
    }
}

// a one-record budget serializes writers and returns the gauge to zero
#[test]
fn budget_backpressure_serializes() {
    let (shared, _sim) = harness_capped(
        config(SyncPolicy::Never),
        FaultPlan::new(1),
        InflightBudget::new(ByteCount::from_bytes(framed(250))),
    );
    let appender = Arc::new(Appender::open(Arc::clone(&shared), 0, None).expect("open"));

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
    let mut settings = config(SyncPolicy::Bytes(ByteCount::gb(1)));
    settings.segment_bytes = ByteCount::mb(256);
    let (shared, sim) = harness(settings, FaultPlan::new(1));
    let appender = Appender::open(Arc::clone(&shared), 0, None).expect("open");

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
