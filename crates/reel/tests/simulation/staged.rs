//! Writes stopped between their claim and their row, and reads stopped between their steps

use std::collections::BTreeMap;
use std::io::IoSlice;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread;

use crate::seed_sets::seeds;
use reel::config::{ColumnConfig, KeyShape, StoreConfig, SyncPolicy, VolumeConfig};
use reel::engine::claim::Claim;
use reel::engine::engine_state::EngineState;
use reel::engine::open_segment::{data_path, rows_path};
use reel::engine::read_path::{KeyRead, RecordWant};
use reel::engine::recovery::seal_every_open;
use reel::engine::reel_store::ReelStore;
use reel::engine::write_path::{ClaimRequest, LandOutcome};
use reel::guard::hold;
use reel::index::open_slots::OpenSlot;
use reel::index::write_row::WriteRow;
use reel_sim::file_image::BitMask;
use reel_sim::seeded_random::{Seed, SeededRandom};
use reel_sim::simulated_disk::OperationCount;
use reel_sim::simulated_io::SimulatedIo;
use reel_sim::simulated_task::JobIndex;
use reel::io::{Io, IoFile};
use reel::record::codec::Codec;
use reel::record::record_check::{RecordIdentity, RecordKey};
use reel::record::record_encoder::RecordEncoder;
use reel::row_kind::RowKind;
use reel::segment::footer_reader::FooterReader;
use reel::segment::range_covers::Cover;
use reel::segment::segment_tail::SECOND_TAIL_DISTANCE;
use reel::superblock::Superblock;
use reel_api::column_id::ColumnId;
use reel_api::store_error::StoreError;
use reel_api::units::{ByteCount, FileOffset, Nanoseconds, RowCount, VolumeId, WritePlace};

const ROOT: &str = "/disk/staged";
const COLUMN: ColumnId = ColumnId(1);
const LONG_COLUMN: ColumnId = ColumnId(2);
const BUDGET: ByteCount = ByteCount(79 * 1024 * 1024);
const SMALL_BUDGET: ByteCount = ByteCount(68 * 1024 * 1024);
const LARGEST: ByteCount = ByteCount(1024 * 1024);
const LONG_KEY_BYTES: usize = 1_000;
const LONG_KEYS: u64 = 6_000;
const SETTLE_STEPS: u64 = 2_000_000;
const SETTLE_TICK: Nanoseconds = Nanoseconds(1_000_000);
const ROW_PERIOD: Nanoseconds = Nanoseconds(60_000_000);
const SEED_COUNT: u64 = 16;
// seeds a sweep found, kept so CI runs them
const FOUND_SEEDS: [u64; 1] = [314];
const STEPS: u64 = 4_000;
const KEY_SPACE: u64 = 40;
const MOST_IN_FLIGHT: usize = 6;
const MANY_ROWS: u64 = 42_000;
const SHRINK_ROWS: u64 = 15_000;
const SPLIT_ROWS: u64 = 1_000;
const CRASH_ODDS: u64 = 300;
const FLUSH_ODDS: u64 = 60;
const SEAL_JOB: JobIndex = JobIndex(1);
const MOST_SEAL_STEPS: u64 = 10_000;
const PAST_THE_OPEN: u64 = 1_000_000_000;
const SHRINK_CRASH_END: u64 = 5_400;
const SHRINK_CRASH_STEP: usize = 197;
const JOB_ROUNDS: u64 = 20;
const MOST_OPEN: usize = 4;
const MOST_LISTED: usize = 16;

type Store = ReelStore<SimulatedIo>;
type State = EngineState<SimulatedIo>;

/// A put or delete stopped somewhere between its claim and its end
struct Staged<'state> {
    claim: Claim<'state, SimulatedIo>,
    key: Vec<u8>,
    record: Vec<u8>,
    kind: RowKind,
    is_written: bool,
    is_landed: bool,
}

fn config(budget: ByteCount) -> StoreConfig {
    StoreConfig {
        memory_budget: budget,
        threads: Default::default(),
        volumes: vec![VolumeConfig {
            name: "meta".to_string(),
            roots: vec![PathBuf::from(ROOT)],
            sync: SyncPolicy::Never,
        }],
        columns: vec![
            ColumnConfig {
                id: COLUMN,
                name: "track".to_string(),
                key_shape: KeyShape::Variable { most: 64 },
                codec: Codec::None,
                volume: VolumeId(0),
                complete_rows: None,
            },
            ColumnConfig {
                id: LONG_COLUMN,
                name: "long".to_string(),
                key_shape: KeyShape::Variable { most: 1_056 },
                codec: Codec::None,
                volume: VolumeId(0),
                complete_rows: None,
            },
        ],
    }
}

fn open(io: &SimulatedIo) -> Store {
    ReelStore::open(io.clone(), &config(BUDGET)).expect("open")
}

fn read(store: &Store, key: &[u8]) -> Option<Vec<u8>> {
    let mut buf = Vec::new();
    match store.get(COLUMN, key, &mut buf).expect("get") {
        true => Some(buf),
        false => None,
    }
}

fn settle(io: &SimulatedIo) {
    for _step in 0..SETTLE_STEPS {
        if io.step_any().is_none() {
            io.advance_clock(SETTLE_TICK);
            if io.step_any().is_none() {
                return;
            }
        }
    }
    panic!("jobs never went quiet");
}

// let jobs run for a while, for runs where a seal waits on a write the test is holding
fn let_jobs_run(io: &SimulatedIo) {
    for _round in 0..JOB_ROUNDS {
        for _step in 0..SETTLE_STEPS {
            if io.step_any().is_none() {
                break;
            }
        }
        io.advance_clock(SETTLE_TICK);
    }
}

fn data_files(io: &SimulatedIo) -> Vec<PathBuf> {
    let mut files = Vec::new();
    for path in io.list_directory(&PathBuf::from(ROOT)).expect("list") {
        if path.extension().is_some_and(|extension| extension == "data") {
            files.push(path);
        }
    }
    files
}

fn stage<'state>(state: &'state State, key: &[u8], value: Option<&[u8]>) -> Staged<'state> {
    let volume = &state.volumes[0];
    // a claim beside busy writers takes a segment made ahead, so have one ready
    hold(&volume.claims).wants_prepared = true;
    state.prepare_segments();
    let kind = if value.is_some() { RowKind::Value } else { RowKind::Grave };
    let length = value.map_or(0, |value| encode(state, key, value, WritePlace(0)).len());
    let request = ClaimRequest {
        bytes: ByteCount(length as u64),
        rows: RowCount(1),
        is_alone: false,
        has_records: true,
        covers: 0,
    };
    let claim = state.claim(volume, &request).expect("claim");
    let record = value.map_or(Vec::new(), |value| encode(state, key, value, claim.first_place()));
    Staged {
        claim,
        key: key.to_vec(),
        record,
        kind,
        is_written: false,
        is_landed: false,
    }
}

fn encode(state: &State, key: &[u8], value: &[u8], place: WritePlace) -> Vec<u8> {
    let identity = RecordIdentity {
        record_key: state.record_key,
        column: COLUMN,
        key,
        kind: RowKind::Value,
        place,
    };
    let mut encoder = RecordEncoder::new(LARGEST, 0);
    let shape = encoder.encode(&identity, Codec::None, value).expect("encode");
    let mut record = Vec::new();
    for part in encoder.parts(&shape, value) {
        record.extend_from_slice(&part);
    }
    record
}

impl Staged<'_> {
    fn place(&self) -> WritePlace {
        self.claim.first_place()
    }

    fn slot(&self) -> OpenSlot {
        self.claim.slot
    }

    fn write(&mut self) {
        let parts = [IoSlice::new(&self.record)];
        if !self.record.is_empty() {
            self.claim.data.write_at(self.claim.offset, &parts).expect("write");
        }
        self.is_written = true;
    }

    fn try_land(&mut self, state: &State) -> bool {
        let row = WriteRow {
            place: self.claim.first_place(),
            offset: self.claim.offset,
            length: ByteCount(self.record.len() as u64),
            kind: self.kind,
            slot: self.claim.slot,
            segment: self.claim.segment,
            has_newer_sealed: false,
        };
        let outcome = state.land_row(COLUMN, &self.key, &row).expect("land");
        self.is_landed = outcome == LandOutcome::Landed;
        self.is_landed
    }

    fn land(&mut self, state: &State) {
        assert!(self.try_land(state));
    }

    fn finish(mut self, state: &State) {
        if !self.is_written {
            self.write();
        }
        if !self.is_landed {
            self.land(state);
        }
        self.claim.finish();
    }
}

// a row that lands during a sync is never covered by it without its record, so it reads back or it's gone
#[test]
fn sync_order() {
    for seed in seeds(0..SEED_COUNT) {
        let io = SimulatedIo::new(Seed(seed));
        let store = open(&io);
        store.put(COLUMN, b"durable", b"first").expect("put");
        store.flush().expect("flush");
        store.put(COLUMN, b"key", b"older").expect("put");
        store.flush().expect("flush");

        let state = Arc::clone(&store.state);
        io.after_next_sync(Box::new(move || {
            state.write_value(COLUMN, b"key", b"landed-during-the-sync").expect("put");
        }));
        store.flush().expect("flush");
        store.put(COLUMN, b"after", b"carries the sync mark").expect("put");
        settle(&io);
        io.advance_clock(ROW_PERIOD);
        settle(&io);

        io.power_cut();
        drop(store);

        let store = open(&io);
        match read(&store, b"key") {
            Some(value) => {
                assert!(value == b"older" || value == b"landed-during-the-sync");
            },
            None => panic!("a flushed value went missing"),
        }
        assert_eq!(read(&store, b"durable"), Some(b"first".to_vec()));
        store.close().expect("close");
    }
}

// a row write that fails keeps its group, and the rows reach the file on the next pass
#[test]
fn row_retry() {
    let io = SimulatedIo::new(Seed(1));
    let store = open(&io);
    store.put(COLUMN, b"key", b"value").expect("put");

    io.fail_writes(OperationCount(1));
    settle(&io);
    assert!(matches!(store.put(COLUMN, b"other", b"value"), Err(StoreError::VolumeFailed { .. })));

    io.advance_clock(ROW_PERIOD);
    settle(&io);
    io.kill();
    drop(store);

    let store = open(&io);
    assert_eq!(read(&store, b"key"), Some(b"value".to_vec()));
    store.close().expect("close");
}

// long keys fill the write index before any row limit, and a seal still makes room
#[test]
fn full_index() {
    let io = SimulatedIo::new(Seed(2));
    let store = open(&io);

    let mut key = vec![b'k'; LONG_KEY_BYTES];
    for number in 0..LONG_KEYS {
        key[..8].copy_from_slice(&number.to_le_bytes());
        store.put(LONG_COLUMN, &key, b"value").expect("put");
    }

    let mut buf = Vec::new();
    key[..8].copy_from_slice(&0_u64.to_le_bytes());
    assert!(store.get(LONG_COLUMN, &key, &mut buf).expect("get"));
    store.close().expect("close");
}

// rot in a sealed segment's tail keeps the file and serves every other segment
#[test]
fn tail_rot() {
    let io = SimulatedIo::new(Seed(3));
    let store = open(&io);
    store.put(COLUMN, b"first", b"value").expect("put");
    store.seal_now();
    settle(&io);
    store.put(COLUMN, b"second", b"value").expect("put");
    store.seal_now();
    settle(&io);
    store.close().expect("close");
    drop(store);

    let files = data_files(&io);
    assert_eq!(files.len(), 2);
    let file = io.open_read_only(&files[0]).expect("open");
    let length = file.length().expect("length");
    io.close_file(file);
    io.rot(&files[0], FileOffset(length.0 - 3), BitMask(1)).expect("rot");
    let second = FileOffset(length.0 - SECOND_TAIL_DISTANCE - 3);
    io.rot(&files[0], second, BitMask(1)).expect("rot");

    let store = open(&io);
    assert_eq!(store.damaged_segments(), 1);
    assert_eq!(data_files(&io).len(), 2);
    assert_eq!(read(&store, b"second"), Some(b"value".to_vec()));
    store.close().expect("close");
}

// rot in a sealed segment's block index keeps the file and serves every other segment
#[test]
fn index_rot() {
    let io = SimulatedIo::new(Seed(4));
    let store = open(&io);
    store.put(COLUMN, b"first", b"value").expect("put");
    store.seal_now();
    settle(&io);
    store.put(COLUMN, b"second", b"value").expect("put");
    store.seal_now();
    settle(&io);
    let at = store.state.volumes[0].view();
    assert_eq!(at.0, 0);
    let first = data_files(&io);
    store.close().expect("close");
    drop(store);

    let file = io.open_read_only(&first[0]).expect("open");
    let mut reader = FooterReader::new();
    let mut columns = Vec::new();
    let key = load_key(&io);
    let head = reader.read_tail(&file, key, &mut columns).expect("tail").expect("sealed");
    io.close_file(file);
    io.rot(&first[0], FileOffset(head.index_at.0 + 2), BitMask(1)).expect("rot");
    io.rot(&first[0], FileOffset(head.second_index_at.0 + 2), BitMask(1)).expect("rot");

    let store = open(&io);
    assert_eq!(store.damaged_segments(), 1);
    assert_eq!(read(&store, b"second"), Some(b"value".to_vec()));
    store.close().expect("close");
}

fn load_key(io: &SimulatedIo) -> RecordKey {
    let superblock = Superblock::read(io, &PathBuf::from(ROOT)).expect("read");
    let superblock = superblock.expect("superblock");
    superblock.record_key
}

// a data file that never took a record goes at open, with no rows file beside it
#[test]
fn empty_orphan() {
    let io = SimulatedIo::new(Seed(5));
    let store = open(&io);
    store.close().expect("close");
    drop(store);

    let path = PathBuf::from(ROOT).join("0000000000abcdef.data");
    let file = io.create_file(&path, ByteCount(4_096)).expect("create");
    io.close_file(file);

    let store = open(&io);
    assert_eq!(store.damaged_segments(), 0);
    assert!(data_files(&io).is_empty());
    store.close().expect("close");
}

// a recovered open segment goes on taking writes, as it would have with no reopen
#[test]
fn stays_open() {
    let io = SimulatedIo::new(Seed(6));
    let store = open(&io);
    store.put(COLUMN, b"first", b"value").expect("put");
    settle(&io);
    io.advance_clock(ROW_PERIOD);
    settle(&io);
    io.kill();
    drop(store);

    let store = open(&io);
    store.put(COLUMN, b"second", b"value").expect("put");
    settle(&io);

    assert_eq!(data_files(&io).len(), 1);
    assert_eq!(read(&store, b"first"), Some(b"value".to_vec()));
    assert_eq!(read(&store, b"second"), Some(b"value".to_vec()));
    store.close().expect("close");
}

// puts during the row job's wait leave it asleep, so a period's rows go out together
#[test]
fn row_batches() {
    let io = SimulatedIo::new(Seed(7));
    let store = open(&io);
    settle(&io);
    store.put(COLUMN, b"wakes-it", b"value").expect("put");
    assert!(io.step_any().is_some());
    settle(&io);

    for number in 0..100_u64 {
        store.put(COLUMN, &number.to_le_bytes(), b"value").expect("put");
        assert_eq!(io.step_any(), None);
    }

    io.advance_clock(ROW_PERIOD);
    assert!(io.step_any().is_some());
    store.close().expect("close");
}

// a create that fails frees its slot and leaves no file, however often it happens
#[test]
fn create_failure() {
    let io = SimulatedIo::new(Seed(8));
    let store = open(&io);
    let slots = store.state.sizing.shard.open_slots as u64;

    for _attempt in 0..slots * 2 {
        io.fail_creates(OperationCount(1));
        assert!(store.put(COLUMN, b"key", b"value").is_err());
    }

    store.put(COLUMN, b"key", b"value").expect("put");
    assert_eq!(data_files(&io).len(), 1);
    store.close().expect("close");
}

fn fill_numbers(store: &Store, rows: u64) {
    for number in 0..rows {
        store.put(COLUMN, &number.to_le_bytes(), &number.to_be_bytes()).expect("put");
    }
}

fn check_numbers(store: &Store, rows: u64) {
    for number in 0..rows {
        assert_eq!(read(store, &number.to_le_bytes()), Some(number.to_be_bytes().to_vec()));
    }
}

// a smaller budget opens by sealing and copying, with every row still there
#[test]
fn smaller_budget() {
    let io = SimulatedIo::new(Seed(9));
    let store = open(&io);
    fill_numbers(&store, MANY_ROWS);
    store.close().expect("close");
    drop(store);

    let store = ReelStore::open(io.clone(), &config(SMALL_BUDGET)).expect("open");
    check_numbers(&store, MANY_ROWS);
    store.put(COLUMN, b"after", b"value").expect("put");
    store.close().expect("close");
    drop(store);

    let store = open(&io);
    check_numbers(&store, MANY_ROWS);
    assert_eq!(read(&store, b"after"), Some(b"value".to_vec()));
    store.close().expect("close");
    assert_eq!(io.open_handle_count(), 0);
    assert_eq!(io.handle_faults(), OperationCount(0));
}

// a crash anywhere in such an open loses nothing, and the next open finishes the job
#[test]
fn smaller_budget_crash() {
    let mut crash_points: Vec<u64> = (0..SHRINK_CRASH_END).step_by(SHRINK_CRASH_STEP).collect();
    crash_points.push(PAST_THE_OPEN);
    for crash_at in crash_points {
        for is_power_cut in [false, true] {
            let io = SimulatedIo::new(Seed(crash_at));
            let store = open(&io);
            fill_numbers(&store, SHRINK_ROWS);
            store.close().expect("close");
            drop(store);

            io.arm_crash(OperationCount(crash_at));
            let interrupted = ReelStore::open(io.clone(), &config(SMALL_BUDGET));
            if is_power_cut {
                io.power_cut();
            } else {
                io.kill();
            }
            drop(interrupted);

            let store = ReelStore::open(io.clone(), &config(SMALL_BUDGET)).expect("open");
            check_numbers(&store, SHRINK_ROWS);
            store.close().expect("close");
        }
    }
}

// a seal holds its switch for a slower write of a lower place, so the later delete still wins
#[test]
fn late_row() {
    let io = SimulatedIo::new(Seed(11));
    let store = open(&io);
    let state = Arc::clone(&store.state);

    let slow_put = stage(&state, b"key", Some(b"earlier"));
    let held_open = stage(&state, b"other", Some(b"value"));
    let delete = stage(&state, b"key", None);
    assert_ne!(delete.slot(), slow_put.slot());
    assert!(delete.place() > slow_put.place());

    let delete_slot = delete.slot();
    delete.finish(&state);
    state.seal_slot(&state.volumes[0], delete_slot);
    let_jobs_run(&io);
    assert!(state.volumes[0].view().contains(delete_slot));

    slow_put.finish(&state);
    held_open.finish(&state);
    settle(&io);

    assert!(!state.volumes[0].view().contains(delete_slot));
    assert_eq!(read(&store, b"key"), None);
    store.close().expect("close");
}

// a get held below a sealing row can't leave its older value in the spot cache
#[test]
fn low_read_point() {
    let io = SimulatedIo::new(Seed(12));
    let store = open(&io);
    let state = Arc::clone(&store.state);
    store.put(COLUMN, b"key", b"older").expect("put");
    store.seal_now();
    settle(&io);

    let holds_mark = stage(&state, b"held", Some(b"value"));
    let holds_open = stage(&state, b"open", Some(b"value"));
    let newer = stage(&state, b"key", Some(b"newer"));
    let newer_slot = newer.slot();
    newer.finish(&state);
    state.seal_slot(&state.volumes[0], newer_slot);
    let_jobs_run(&io);

    assert_eq!(read(&store, b"key"), Some(b"older".to_vec()));
    assert_eq!(read(&store, b"key"), Some(b"older".to_vec()));

    holds_mark.finish(&state);
    holds_open.finish(&state);
    settle(&io);

    assert_eq!(read(&store, b"key"), Some(b"newer".to_vec()));
    store.close().expect("close");
}

// a get stopped between its lookup and its pin finds the slot moved on, and goes round again
#[test]
fn reused_slot() {
    let io = SimulatedIo::new(Seed(13));
    let store = open(&io);
    let state = Arc::clone(&store.state);
    store.put(COLUMN, b"key", b"value").expect("put");

    let volume = &state.volumes[0];
    let point = state.read_point();
    let found = state.index.lookup(COLUMN, b"key", point, volume.view());
    let row = found.row.expect("row");

    store.seal_now();
    settle(&io);
    store.put(COLUMN, b"other", b"a value of another length").expect("put");
    assert!(volume.view().contains(row.slot));

    let read_key: KeyRead = state.key_read(COLUMN, b"key", point, false).expect("key");
    let mut buf = Vec::new();
    let is_read = state.read_open(&read_key, &row, RecordWant::Whole, &mut buf).expect("no corruption");

    assert!(!is_read);
    assert_eq!(read(&store, b"key"), Some(b"value".to_vec()));
    store.close().expect("close");
}

fn stage_done(state: &State, key: &[u8], value: Option<&[u8]>) {
    stage(state, key, value).finish(state);
}

// a seal that hits a full disk part way starts its footer again and finishes
#[test]
fn seal_retry() {
    let io = SimulatedIo::new(Seed(10));
    let store = open(&io);
    store.put(COLUMN, b"key", b"value").expect("put");
    let slot = store.state.index.lookup(COLUMN, b"key", store.state.read_point(), store.state.volumes[0].view());
    let slot = slot.row.expect("row").slot;

    // the rows go out first, so the failed write is the footer's
    settle(&io);
    io.advance_clock(ROW_PERIOD);
    settle(&io);

    store.seal_now();
    io.fill_writes(OperationCount(1));
    settle(&io);
    assert!(!store.state.volumes[0].has_failed());
    for _try in 0..JOB_ROUNDS {
        io.advance_clock(ROW_PERIOD);
        settle(&io);
    }

    assert!(!store.state.volumes[0].view().contains(slot));
    assert_eq!(read(&store, b"key"), Some(b"value".to_vec()));
    store.close().expect("close");
}

// a seal that fails after the table has it finishes with one entry, not one per retry
#[test]
fn install_retry() {
    let io = SimulatedIo::new(Seed(15));
    let store = open(&io);
    store.put(COLUMN, b"key", b"value").expect("put");
    settle(&io);
    io.advance_clock(ROW_PERIOD);
    settle(&io);

    store.seal_now();
    for _step in 0..MOST_SEAL_STEPS {
        // the discard is the seal's first directory sync once its footer is durable
        if store.state.volumes[0].table_holds_any() {
            break;
        }
        assert!(io.step_job(SEAL_JOB).is_some());
    }
    io.fail_directory_syncs(OperationCount(1));
    for _try in 0..JOB_ROUNDS {
        io.advance_clock(ROW_PERIOD);
        settle(&io);
    }

    assert_eq!(store.state.volumes[0].view().0, 0);
    let mut listed = Vec::with_capacity(MOST_LISTED);
    let directory = store.state.directories[COLUMN.0 as usize].as_ref().expect("directory");
    directory.candidates(b"key", 0, &mut listed);
    assert_eq!(listed.len(), 1);
    assert_eq!(read(&store, b"key"), Some(b"value".to_vec()));
    store.close().expect("close");
}

// an older row replayed beside a sealed newer one can't win, through a close or a kill
#[test]
fn reopened_older() {
    for is_kill in [false, true] {
        let io = SimulatedIo::new(Seed(16));
        let store = open(&io);
        let state = Arc::clone(&store.state);

        let older = stage(&state, b"key", Some(b"older"));
        let hold_open = stage(&state, b"hold", Some(b"value"));
        let newer = stage(&state, b"key", Some(b"newer"));
        let newer_slot = newer.slot();
        assert_ne!(newer_slot, older.slot());
        older.finish(&state);
        hold_open.finish(&state);
        newer.finish(&state);

        state.seal_slot(&state.volumes[0], newer_slot);
        settle(&io);
        assert_eq!(read(&store, b"key"), Some(b"newer".to_vec()));

        io.advance_clock(ROW_PERIOD);
        settle(&io);
        if is_kill {
            io.kill();
        } else {
            store.close().expect("close");
        }
        drop(state);
        drop(store);

        let store = open(&io);
        assert_eq!(read(&store, b"key"), Some(b"newer".to_vec()));
        store.close().expect("close");
    }
}

// a replayed older grave can't stand in for a delete of the newer sealed value
#[test]
fn reopened_grave() {
    let io = SimulatedIo::new(Seed(17));
    let store = open(&io);
    let state = Arc::clone(&store.state);

    let grave = stage(&state, b"key", None);
    let hold_open = stage(&state, b"hold", Some(b"value"));
    let newer = stage(&state, b"key", Some(b"newer"));
    let newer_slot = newer.slot();
    grave.finish(&state);
    hold_open.finish(&state);
    newer.finish(&state);
    state.seal_slot(&state.volumes[0], newer_slot);
    settle(&io);
    store.close().expect("close");
    drop(state);
    drop(store);

    let store = open(&io);
    store.delete(COLUMN, b"key").expect("delete");
    assert_eq!(read(&store, b"key"), None);
    store.seal_now();
    settle(&io);
    store.close().expect("close");
    drop(store);

    let store = open(&io);
    assert_eq!(read(&store, b"key"), None);
    store.close().expect("close");
}

// a segment sealed during open can't let an older row replayed after it win
#[test]
fn sealed_in_open() {
    let io = SimulatedIo::new(Seed(14));
    let store = open(&io);
    let state = Arc::clone(&store.state);

    let first_hold = stage(&state, b"hold-one", Some(b"value"));
    let second_hold = stage(&state, b"hold-two", Some(b"value"));
    let older = stage(&state, b"key", Some(b"older"));
    let third_hold = stage(&state, b"hold-three", Some(b"value"));
    let older_slot = older.slot();
    assert_ne!(older_slot, first_hold.slot());
    assert_eq!(older_slot, third_hold.slot());
    older.finish(&state);
    for number in 0..SPLIT_ROWS {
        stage_done(&state, &number.to_be_bytes(), Some(b"in the second segment"));
    }

    first_hold.finish(&state);
    let newer = stage(&state, b"key", Some(b"newer"));
    assert_ne!(newer.slot(), older_slot);
    newer.finish(&state);
    second_hold.finish(&state);
    third_hold.finish(&state);
    for number in 0..SPLIT_ROWS {
        stage_done(&state, &number.to_le_bytes(), Some(b"in the first segment"));
    }
    store.close().expect("close");
    drop(state);
    drop(store);

    let store = ReelStore::open(io.clone(), &config(SMALL_BUDGET)).expect("open");
    assert_eq!(read(&store, b"key"), Some(b"newer".to_vec()));
    store.close().expect("close");
}

// a sync reaches every open segment even when a seal removes one beside it
#[test]
fn sync_beside_seal() {
    for seed in seeds(0..SEED_COUNT) {
        let io = SimulatedIo::new(Seed(seed));
        let store = open(&io);
        let state = Arc::clone(&store.state);

        let sealing = stage(&state, b"first", Some(b"value"));
        let hold_open = stage(&state, b"hold", Some(b"value"));
        let beside = stage(&state, b"second", Some(b"value"));
        let sealing_slot = sealing.slot();
        assert_ne!(beside.slot(), sealing_slot);
        sealing.finish(&state);
        hold_open.finish(&state);
        beside.finish(&state);

        let volume = &state.volumes[0];
        let before = volume.handover_count();
        state.seal_slot(volume, sealing_slot);
        for _step in 0..MOST_SEAL_STEPS {
            if volume.handover_count() != before {
                break;
            }
            assert!(io.step_job(SEAL_JOB).is_some());
        }

        let hook_io = io.clone();
        let hook_state = Arc::clone(&state);
        io.after_next_sync(Box::new(move || {
            for _step in 0..MOST_SEAL_STEPS {
                if !hook_state.volumes[0].view().contains(sealing_slot) {
                    break;
                }
                hook_io.step_job(SEAL_JOB);
            }
        }));
        store.flush().expect("flush");
        assert!(!volume.view().contains(sealing_slot));

        io.power_cut();
        drop(state);
        drop(store);

        let store = open(&io);
        assert_eq!(read(&store, b"second"), Some(b"value".to_vec()));
        assert_eq!(read(&store, b"first"), Some(b"value".to_vec()));
        store.close().expect("close");
    }
}

/// A get stopped between its lookup and its read
struct StagedGet {
    key: Vec<u8>,
    point: WritePlace,
    handovers: u64,
    reclaims: u64,
    row: Option<WriteRow>,
    should_ask: bool,
}

type History = BTreeMap<Vec<u8>, BTreeMap<u64, Option<Vec<u8>>>>;

fn newest_at(history: &History, key: &[u8], point: u64) -> Option<Vec<u8>> {
    let versions = history.get(key)?;
    let (_place, value) = versions.range(..=point).next_back()?;
    value.clone()
}

fn begin_get(state: &State, key: &[u8]) -> StagedGet {
    let volume = &state.volumes[0];
    let reclaims = state.cues.counts().reclaims;
    let handovers = volume.handover_count();
    let point = state.read_point();
    let found = state.index.lookup(COLUMN, key, point, volume.view());
    StagedGet {
        key: key.to_vec(),
        point,
        handovers,
        reclaims,
        row: found.row,
        should_ask: found.should_ask_footers,
    }
}

fn finish_get(state: &State, get: &StagedGet) -> Option<Vec<u8>> {
    let read: KeyRead = state.key_read(COLUMN, &get.key, get.point, false).expect("key");
    let mut buf = Vec::new();
    if let Some(row) = get.row
        && !get.should_ask
    {
        if row.kind != RowKind::Value {
            return None;
        }
        if state.read_open(&read, &row, RecordWant::Whole, &mut buf).expect("read") {
            return Some(buf);
        }
    }

    match state.get_at(&read, get.handovers, RecordWant::Whole, &mut buf).expect("get") {
        true => Some(buf),
        false => None,
    }
}

// staged puts, deletes and gets with seals, jobs, syncs, kills and power cuts in the order a seed
// picks: every get sees the newest row at its read point, and a crash keeps what a sync covered
#[test]
fn seeded_interleaving() {
    for seed in seeds((0..SEED_COUNT).chain(FOUND_SEEDS)) {
        run_interleaving(Seed(seed));
    }
}

fn run_interleaving(seed: Seed) {
    let io = SimulatedIo::new(seed);
    let mut random = SeededRandom::new(seed);
    let mut history: History = BTreeMap::new();
    let mut floors: BTreeMap<Vec<u8>, u64> = BTreeMap::new();
    let mut steps_left = STEPS;

    while steps_left > 0 {
        let store = open(&io);
        let state = Arc::clone(&store.state);
        let crash = run_until_crash(&io, &store, &state, &mut random, &mut history, &mut floors, &mut steps_left);
        match crash {
            Some(is_power_cut) => {
                if is_power_cut {
                    io.power_cut();
                } else {
                    io.kill();
                }
            },
            None => {
                settle(&io);
                let mark = state.read_point().0;
                for key in history.keys() {
                    assert_eq!(read(&store, key), newest_at(&history, key, mark));
                }
                store.close().expect("close");
                assert_eq!(io.open_handle_count(), 0);
            },
        }
        drop(state);
        drop(store);

        if crash.is_some() {
            let store = open(&io);
            reconcile(&store, &mut history, &floors);
            store.close().expect("close");
        }
    }
    assert_eq!(io.handle_faults(), OperationCount(0));
}

// take what each key reads after a crash as the truth, once it's at or above what a sync covered
fn reconcile(store: &Store, history: &mut History, floors: &BTreeMap<Vec<u8>, u64>) {
    for (key, versions) in history.iter_mut() {
        let found = read(store, key);
        let floor = floors.get(key).copied().unwrap_or(0);
        if floor == 0 && found.is_none() {
            // nothing a sync covered, and nothing survived: every version of this key is gone
            versions.clear();
            continue;
        }

        let mut accepted = None;
        for (place, value) in versions.range(floor..) {
            if *value == found {
                accepted = Some(*place);
                // two deletes read alike, and the lower one keeps no place a later write could pass
                break;
            }
        }

        let Some(accepted) = accepted else {
            panic!("key {key:?} read {found:?} below its floor {floor}");
        };
        versions.retain(|place, _value| *place >= floor && *place <= accepted);
    }
}

// run seeded steps until the steps run out, and say which crash ended them, if one did
fn run_until_crash(
    io: &SimulatedIo,
    store: &Store,
    state: &State,
    random: &mut SeededRandom,
    history: &mut History,
    floors: &mut BTreeMap<Vec<u8>, u64>,
    steps_left: &mut u64,
) -> Option<bool> {
    let mut in_flight: Vec<Staged> = Vec::new();
    let mut gets: Vec<StagedGet> = Vec::new();

    while *steps_left > 0 {
        *steps_left -= 1;
        if random.one_in(CRASH_ODDS) {
            return Some(random.one_in(2));
        }
        if random.one_in(FLUSH_ODDS) {
            let mark = state.read_point().0;
            store.flush().expect("flush");
            for (key, versions) in history.iter() {
                if let Some((place, _value)) = versions.range(..=mark).next_back() {
                    floors.insert(key.clone(), *place);
                }
            }
        }

        match random.below(12) {
            0..3 => {
                if in_flight.len() < MOST_IN_FLIGHT {
                    let key = format!("key-{:04}", random.below(KEY_SPACE)).into_bytes();
                    let value = if random.one_in(3) {
                        None
                    } else {
                        Some(format!("value-{}", random.next_word()).into_bytes())
                    };
                    let staged = stage(state, &key, value.as_deref());
                    history.entry(key).or_default().insert(staged.place().0, value);
                    in_flight.push(staged);
                }
            },
            3..5 => {
                if !in_flight.is_empty() {
                    let index = random.below(in_flight.len() as u64) as usize;
                    let mut staged = in_flight.swap_remove(index);
                    if !staged.is_written {
                        staged.write();
                        in_flight.push(staged);
                    } else if !staged.is_landed {
                        if staged.try_land(state) {
                            in_flight.push(staged);
                        } else if let Some(versions) = history.get_mut(&staged.key) {
                            // the index was full, so this write ends as aborted
                            versions.remove(&staged.place().0);
                        }
                    } else {
                        staged.finish(state);
                    }
                }
            },
            5 => {
                let open_count = hold(&state.volumes[0].claims).open.len();
                if random.one_in(8) && open_count < MOST_OPEN {
                    store.seal_now();
                }
            },
            6..8 => {
                io.step_any();
                io.advance_clock(SETTLE_TICK);
            },
            8 => {
                let key = format!("key-{:04}", random.below(KEY_SPACE)).into_bytes();
                gets.push(begin_get(state, &key));
            },
            9 => {
                if !gets.is_empty() {
                    let index = random.below(gets.len() as u64) as usize;
                    let get = gets.swap_remove(index);
                    let found = finish_get(state, &get);
                    // a reclaim that crossed a get sends the real one back for a newer point
                    if state.cues.counts().reclaims == get.reclaims {
                        assert_eq!(found, newest_at(history, &get.key, get.point.0), "staged get of {:?}", get.key);
                    }
                }
            },
            _read => {
                let key = format!("key-{:04}", random.below(KEY_SPACE)).into_bytes();
                let mark = state.read_point().0;
                let found = read(store, &key);
                if state.read_point().0 == mark {
                    assert_eq!(found, newest_at(history, &key, mark), "get of {key:?}");
                }
            },
        }
    }

    for staged in in_flight {
        staged.finish(state);
    }
    None
}

// a segment with no rows leaves with the rest, so sealing every open segment ends
#[test]
fn empty_seal() {
    let io = SimulatedIo::new(Seed(21));
    let store = open(&io);
    let state = Arc::clone(&store.state);
    let request = ClaimRequest {
        bytes: ByteCount(0),
        rows: RowCount(1),
        is_alone: false,
        has_records: false,
        covers: 0,
    };
    // a claim that lands nothing leaves a segment with no rows in the index
    drop(state.claim(&state.volumes[0], &request).expect("claim"));
    assert!(!hold(&state.volumes[0].claims).open.is_empty());

    seal_every_open(&state, 0).expect("seal");

    assert!(hold(&state.volumes[0].claims).open.is_empty());
    store.close().expect("close");
}

// a bad sector under a synced record on the copy path damages its keys and the open goes through
#[test]
fn copy_bad() {
    let io = SimulatedIo::new(Seed(22));
    let store = open(&io);
    fill_numbers(&store, SHRINK_ROWS);
    store.flush().expect("flush");
    io.kill();
    drop(store);
    let files = data_files(&io);
    io.bad_sector(&files[0], FileOffset(3)).expect("bad");

    let store = ReelStore::open(io.clone(), &config(SMALL_BUDGET)).expect("open");

    let mut refused = 0;
    let mut buf = Vec::new();
    for number in 0..SHRINK_ROWS {
        match store.get(COLUMN, &number.to_be_bytes(), &mut buf) {
            Ok(_is_found) => {},
            Err(StoreError::Corruption { .. }) => refused += 1,
            Err(error) => panic!("key {number} failed: {error}"),
        }
    }
    assert!(refused > 0);
    store.close().expect("close");
}

// a range delete lands behind a newer one that a stalled writer keeps from publishing: a get at the older one's place
// must find its cover, though the newer one holds it
#[test]
fn covers_out_of_order() {
    let io = SimulatedIo::new(Seed(40));
    io.keep_real_time();
    let store = open(&io);
    let state = Arc::clone(&store.state);
    store.put(COLUMN, b"m", b"value").expect("put");
    let volume = &state.volumes[0];
    let request = ClaimRequest {
        bytes: ByteCount(0),
        rows: RowCount(1),
        is_alone: false,
        has_records: true,
        covers: 1,
    };

    assert!(volume.covers.hold(1));
    let older = state.claim(volume, &request).expect("claim");
    let stalled = stage(&state, b"stalled", None);
    let older_row = WriteRow {
        place: older.first_place(),
        offset: older.offset,
        length: ByteCount(0),
        kind: RowKind::Grave,
        slot: older.slot,
        segment: older.segment,
        has_newer_sealed: false,
    };

    thread::scope(|scope| {
        let newer = scope.spawn(|| store.delete_range(COLUMN, b"a", b"zz"));
        while volume.covers.is_empty() {
            thread::yield_now();
        }

        assert_eq!(state.land_row(COLUMN, b"a", &older_row).expect("land"), LandOutcome::Landed);
        volume.covers.add_held(&Cover {
            column: COLUMN,
            start: b"a",
            end: b"z",
            place: older_row.place,
            segment: older_row.segment,
        });
        older.finish();

        assert_eq!(read(&store, b"m"), None);
        stalled.finish(&state);
        newer.join().expect("join").expect("delete");
    });
    assert_eq!(read(&store, b"m"), None);
    store.close().expect("close");
}

// an open segment that took no row goes at open, as a spare a crash left behind does
#[test]
fn empty_open() {
    let io = SimulatedIo::new(Seed(41));
    let store = open(&io);
    store.put(COLUMN, b"key", b"value").expect("put");
    io.kill();
    drop(store);

    let store = open(&io);

    assert!(hold(&store.state.volumes[0].claims).open.is_empty());
    store.close().expect("close");
}

// a rows file of bytes that read and fail their checks isn't a spare's empty file, so its segment stays
#[test]
fn rotted_rows() {
    let io = SimulatedIo::new(Seed(42));
    let store = open(&io);
    store.put(COLUMN, b"key", b"value").expect("put");
    store.flush().expect("flush");
    let number = hold(&store.state.volumes[0].claims).open[0].number;
    io.kill();
    drop(store);
    let rows = rows_path(Path::new(ROOT), number);
    let file = io.open_file(&rows).expect("open");
    let junk = vec![0xFF; file.length().expect("length").0 as usize];
    file.write_at(FileOffset(0), &[IoSlice::new(&junk)]).expect("write");
    file.sync().expect("sync");
    io.close_file(file);

    let store = open(&io);

    let data = data_path(Path::new(ROOT), number).to_path_buf();
    assert!(io.list_directory(Path::new(ROOT)).expect("list").contains(&data));
    store.close().expect("close");
}
