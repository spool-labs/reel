//! Reel tests over the simulated backend

use super::*;

use std::collections::BTreeSet;
use std::sync::Barrier;
use std::thread;

use crate::units::ByteCount;

use crate::config::{SyncPolicy, ThreadBudget, DEFAULT_FD_CACHE};
use crate::format::column::{Codec, ColumnId, ColumnSpec, KeyWidth};
use crate::io::fault::FaultPlan;
use crate::io::sim_backend::SimIo;

const REEL_DIR: &str = "/bulk/reel";
const RECORD: ColumnId = ColumnId(1);

const RECORD_CF: &str = "record";
const BLOB_CF: &str = "blob";
const ARTIFACT_CF: &str = "artifact";

const GROUP_PREFIX_LEN: usize = 2;
const RECORD_KEY_LEN: usize = GROUP_PREFIX_LEN + 32;
const BLOB_KEY_LEN: usize = 32;
const ARTIFACT_KEY_LEN: usize = 24;

/// The columns these tests open the reel with
const TEST_COLUMNS: ColumnSet = &[
    ColumnSpec {
        id: ColumnId(1),
        name: RECORD_CF,
        key_width: KeyWidth::Fixed(RECORD_KEY_LEN as u16),
        shard_bytes: GROUP_PREFIX_LEN as u8,
        purge_mark: None,
        codec: Codec::None,
    },
    ColumnSpec {
        id: ColumnId(2),
        name: BLOB_CF,
        key_width: KeyWidth::Fixed(BLOB_KEY_LEN as u16),
        shard_bytes: 1,
        purge_mark: None,
        codec: Codec::None,
    },
    ColumnSpec {
        id: ColumnId(3),
        name: ARTIFACT_CF,
        key_width: KeyWidth::Fixed(ARTIFACT_KEY_LEN as u16),
        shard_bytes: 0,
        purge_mark: None,
        codec: Codec::None,
    },
];

fn harness(active_tails: usize) -> (Arc<ReelShared>, SimIo) {
    let config = ReelConfig {
        segment_bytes: ByteCount::mb(1),
        sync: SyncPolicy::Never,
        active_tails: ThreadBudget::threads(active_tails as u32),
        ..ReelConfig::default()
    };
    let sim = SimIo::new(FaultPlan::new(1));
    let driver = Arc::new(IoDriver::new(Arc::new(sim.clone())));
    let budget = Arc::new(InflightBudget::default());
    let fd_cache = Arc::new(FdCache::new(DEFAULT_FD_CACHE as usize));
    let shared = Arc::new(ReelShared::new(
        PathBuf::from(REEL_DIR),
        driver,
        budget,
        fd_cache,
        config,
        TEST_COLUMNS,
        1,
    ));
    (shared, sim)
}

fn key(byte: u8) -> RecordKey {
    RecordKey::from_bytes(RECORD, &[byte; 34]).expect("key")
}

// a segment file name is a zero-padded number and the suffix
#[test]
fn names_segment_files() {
    assert_eq!(segment_file_name(SegmentId(1)), "000001.reel");
    assert_eq!(segment_file_name(SegmentId(132)), "000132.reel");
}

// a segment file name parses back to its number and rejects other files
#[test]
fn parses_segment_numbers() {
    assert_eq!(segment_number("000132.reel"), Some(132));
    assert_eq!(segment_number("reel.lock"), None);
}

// a reel refuses to draw past the last segment number and never wraps
#[test]
fn refuses_to_wrap_segment_numbers() {
    let (shared, _sim) = harness(1);
    shared.recover_next_segment(SegmentId(u32::MAX - 2));

    let (id, _holds) = shared.next_segment().expect("last drawable number");
    assert_eq!(id, SegmentId(u32::MAX - 1));
    assert!(shared.next_segment().is_err());
    assert!(shared.next_segment().is_err());
}

// opening a reel starts one appender per active tail, each on its own segment
#[test]
fn opens_configured_tails() {
    let (shared, _sim) = harness(4);
    let reel = Reel::open(shared, Vec::new()).expect("open");

    assert_eq!(reel.tails().len(), 4);
    let segments: BTreeSet<u32> = reel
        .tails()
        .iter()
        .map(|tail| tail.tail().active_segment().as_u32())
        .collect();
    assert_eq!(segments.len(), 4);
}

// each tail draws its own segment number from the one reel counter
#[test]
fn each_tail_writes_its_own_segment() {
    let (shared, _sim) = harness(3);
    let reel = Reel::open(shared, Vec::new()).expect("open");

    for index in 0..3usize {
        let committed = reel.tails()[index]
            .append_data(key(index as u8 + 1), vec![0x55; 128], 0, Commit::PerRecord)
            .expect("append");
        assert_eq!(
            committed.loc.segment,
            reel.tails()[index].tail().active_segment()
        );
    }
}

// put and delete route to a tail, commit, and flush cleanly
#[test]
fn put_and_delete_commit() {
    let (shared, _sim) = harness(1);
    let reel = Reel::open(shared, Vec::new()).expect("open");

    let put = reel
        .put(key(1), vec![0x11; 400], 0, Commit::PerRecord)
        .expect("put");
    assert_eq!(put.lsn, Lsn(1));
    let delete = reel.delete(key(2), Commit::PerRecord).expect("delete");
    assert_eq!(delete.lsn, Lsn(2));

    reel.flush().expect("flush");
}

// a range tombstone commits like any other record
#[test]
fn range_delete_commits() {
    let (shared, _sim) = harness(1);
    let reel = Reel::open(shared, Vec::new()).expect("open");

    let end = [0x05u8; 34];
    let dropped = reel
        .delete_range(key(1), Some(&end), Commit::PerRecord)
        .expect("range delete");

    assert_eq!(dropped.lsn, Lsn(1));
    assert_eq!(dropped.loc.len, end.len() as u32);
}

// a record read back resolves its own key, and another key fails its check
#[test]
fn reads_back_by_key() {
    let (shared, _sim) = harness(1);
    let reel = Reel::open(shared, Vec::new()).expect("open");
    let committed = reel
        .put(key(9), vec![0x99; 300], 0, Commit::PerRecord)
        .expect("put");
    reel.flush().expect("flush");

    let found = reel
        .read_record(committed.loc, key(9).as_ref(), committed.lsn, true, false)
        .expect("read");
    let stale = reel
        .read_record(committed.loc, key(8).as_ref(), committed.lsn, true, false)
        .expect("read");

    assert_eq!(found, RecordRead::Found(Value::new(vec![0x99; 300])));
    assert_eq!(stale, RecordRead::Corrupt);
}

// an old place still reads its own record, since a keyless record holds no version
#[test]
fn a_superseded_place_reads_its_own_record() {
    let (shared, _sim) = harness(1);
    let reel = Reel::open(shared, Vec::new()).expect("open");
    let first = reel
        .put(key(7), vec![0x11; 300], 0, Commit::PerRecord)
        .expect("put");
    let second = reel
        .put(key(7), vec![0x22; 300], 0, Commit::PerRecord)
        .expect("put");
    reel.flush().expect("flush");

    // An index that moved on can pair the old place with the new sequence number
    let superseded = reel
        .read_record(first.loc, key(7).as_ref(), second.lsn, true, false)
        .expect("read");
    let current = reel
        .read_record(second.loc, key(7).as_ref(), second.lsn, true, false)
        .expect("read");

    assert_ne!(first.lsn, second.lsn);
    assert_eq!(superseded, RecordRead::Found(Value::new(vec![0x11; 300])));
    assert_eq!(current, RecordRead::Found(Value::new(vec![0x22; 300])));
}

// concurrent puts across four tails all commit to distinct locations
#[test]
fn concurrent_appends_across_tails() {
    let (shared, _sim) = harness(4);
    let reel = Arc::new(Reel::open(shared, Vec::new()).expect("open"));

    let writers = 16u8;
    let barrier = Arc::new(Barrier::new(writers as usize));
    let mut handles = Vec::new();
    for byte in 0..writers {
        let reel = Arc::clone(&reel);
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            barrier.wait();
            reel.put(key(byte + 1), vec![byte + 1; 300], 0, Commit::PerRecord)
                .expect("put")
        }));
    }
    let mut located = BTreeSet::new();
    for handle in handles {
        let committed = handle.join().expect("join");
        located.insert((committed.loc.segment.as_u32(), committed.loc.offset));
    }
    assert_eq!(located.len(), writers as usize);
    reel.flush().expect("flush");
}

// a keyless record a row placed answers by its shape, and a spot slot's by its check
#[test]
fn a_placed_keyless_read_skips_the_check() {
    use crate::format::record::{
        CheckKey, Flags, RecordHeader, RecordLayout, CHECK_KEY_LEN, KEYLESS_PREFIX,
    };
    use crate::reel::read::{check_in_block, Proof};

    let key = RecordKey::from_bytes(RECORD, &[0x21; RECORD_KEY_LEN]).expect("key");
    let layout = RecordLayout::Keyless(CheckKey::from_bytes([7; CHECK_KEY_LEN]));
    let payload = [9u8; 32];
    let header = RecordHeader::framed(
        layout,
        32,
        Lsn(5),
        Flags::DATA.relocated(),
        key.clone(),
        0,
        &payload,
    );
    let mut block = header.pack_in(layout, &payload).as_slice().to_vec();
    block.extend_from_slice(&payload);
    let loc = Loc::new(SegmentId(1), 0, 32);
    // Read under another segment's key, so the check itself cannot pass
    let other = RecordLayout::Keyless(CheckKey::from_bytes([8; CHECK_KEY_LEN]));
    let read = |loc: Loc, is_verified: bool, is_placed: bool| {
        check_in_block(
            &block,
            0,
            KEYLESS_PREFIX,
            key.as_ref(),
            Lsn(5),
            loc,
            other,
            Proof::of(is_verified, is_placed),
        )
    };

    assert_eq!(
        read(loc, false, true),
        Ok(0),
        "a placed read answers by the shape"
    );
    assert_eq!(
        read(loc, false, false),
        Err(RecordRead::Corrupt),
        "a spot read runs the check"
    );
    assert_eq!(
        read(loc, true, true),
        Err(RecordRead::Corrupt),
        "a verified read runs the check"
    );
    assert_eq!(
        read(Loc::new(SegmentId(1), 0, 31), false, true),
        Err(RecordRead::Stale),
        "a shape off the row is stale"
    );
}
