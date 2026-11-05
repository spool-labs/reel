//! Writes stopped between their claim and their row, and reads stopped between their steps

use std::collections::BTreeMap;
use std::io::IoSlice;
use std::path::PathBuf;
use std::sync::Arc;

use reel::config::{ColumnConfig, KeyShape, StoreConfig, SyncPolicy, VolumeConfig};
use reel::engine::claim::Claim;
use reel::engine::engine_state::EngineState;
use reel::engine::read_path::{KeyRead, RecordWant};
use reel::engine::reel_store::ReelStore;
use reel::engine::write_path::{ClaimRequest, LandOutcome};
use reel::guard::hold;
use reel::index::open_slots::OpenSlot;
use reel::index::write_row::WriteRow;
use reel::io::simulated::file_image::BitMask;
use reel::io::simulated::seeded_random::{Seed, SeededRandom};
use reel::io::simulated::simulated_disk::OperationCount;
use reel::io::simulated::simulated_io::SimulatedIo;
use reel::io::{Io, IoFile};
use reel::record::codec::Codec;
use reel::record::record_check::{RecordIdentity, RecordKey};
use reel::record::record_encoder::RecordEncoder;
use reel::row_kind::RowKind;
use reel::segment::footer_reader::FooterReader;
use reel::superblock::Superblock;
use reel_api::column_id::ColumnId;
use reel_api::store_error::StoreError;
use reel_api::units::{ByteCount, FileOffset, Nanoseconds, RowCount, VolumeId, WritePlace};

const ROOT: &str = "/disk/staged";
const COLUMN: ColumnId = ColumnId(1);
const LONG_COLUMN: ColumnId = ColumnId(2);
const BUDGET: ByteCount = ByteCount(8 * 1024 * 1024);
const SMALL_BUDGET: ByteCount = ByteCount(1024 * 1024);
const LARGEST: ByteCount = ByteCount(1024 * 1024);
const LONG_KEY_BYTES: usize = 1_000;
const LONG_KEYS: u64 = 6_000;
const SETTLE_STEPS: u64 = 2_000_000;
const SETTLE_TICK: Nanoseconds = Nanoseconds(1_000_000);
const ROW_PERIOD: Nanoseconds = Nanoseconds(60_000_000);
const SEED_COUNT: u64 = 24;
const STEPS: u64 = 4_000;
const KEY_SPACE: u64 = 40;
const MOST_IN_FLIGHT: usize = 6;
const MANY_ROWS: u64 = 11_000;
const SHRINK_ROWS: u64 = 2_000;
const SHRINK_CRASH_END: u64 = 3_600;
const SHRINK_CRASH_STEP: usize = 149;
const JOB_ROUNDS: u64 = 20;
const MOST_OPEN: usize = 4;

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
            },
            ColumnConfig {
                id: LONG_COLUMN,
                name: "long".to_string(),
                key_shape: KeyShape::Variable { most: 1_056 },
                codec: Codec::None,
                volume: VolumeId(0),
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
    let (kind, record) = match value {
        Some(value) => (RowKind::Value, encode(state, key, value)),
        None => (RowKind::Grave, Vec::new()),
    };
    let request = ClaimRequest {
        bytes: ByteCount(record.len() as u64),
        rows: RowCount(1),
    };
    let claim = state.claim(volume, &request).expect("claim");
    Staged {
        claim,
        key: key.to_vec(),
        record,
        kind,
        is_written: false,
        is_landed: false,
    }
}

fn encode(state: &State, key: &[u8], value: &[u8]) -> Vec<u8> {
    let identity = RecordIdentity {
        record_key: state.record_key,
        column: COLUMN,
        key,
        kind: RowKind::Value,
    };
    let mut encoder = RecordEncoder::new(LARGEST, LARGEST);
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

    fn land(&mut self, state: &State) {
        let row = WriteRow {
            place: self.claim.first_place(),
            offset: self.claim.offset,
            length: ByteCount(self.record.len() as u64),
            kind: self.kind,
            slot: self.claim.slot,
            segment: self.claim.segment,
        };
        let outcome = state.land_row(COLUMN, &self.key, &row).expect("land");
        assert_eq!(outcome, LandOutcome::Landed);
        self.is_landed = true;
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
    for seed in 0..SEED_COUNT {
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
    for crash_at in (0..SHRINK_CRASH_END).step_by(SHRINK_CRASH_STEP) {
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

// a seal that fails part way starts its footer again and finishes
#[test]
fn seal_retry() {
    let io = SimulatedIo::new(Seed(10));
    let store = open(&io);
    store.put(COLUMN, b"key", b"value").expect("put");
    settle(&io);
    io.advance_clock(ROW_PERIOD);
    settle(&io);

    store.seal_now();
    io.fail_writes(OperationCount(1));
    settle(&io);
    for _try in 0..10 {
        io.advance_clock(ROW_PERIOD);
        settle(&io);
    }

    assert_eq!(read(&store, b"key"), Some(b"value".to_vec()));
    store.close().expect("close");
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

// staged puts, deletes, seals, jobs and gets in the order a seed picks: every get sees the newest row at the mark
#[test]
fn seeded_interleaving() {
    for seed in 0..SEED_COUNT {
        run_interleaving(Seed(seed));
    }
}

fn run_interleaving(seed: Seed) {
    let io = SimulatedIo::new(seed);
    let store = open(&io);
    let state = Arc::clone(&store.state);
    let mut random = SeededRandom::new(seed);
    let mut in_flight: Vec<Staged> = Vec::new();
    let mut versions: BTreeMap<Vec<u8>, BTreeMap<u64, Option<Vec<u8>>>> = BTreeMap::new();

    for step in 0..STEPS {
        match random.below(10) {
            0..3 => {
                if in_flight.len() < MOST_IN_FLIGHT {
                    let key = format!("key-{:04}", random.below(KEY_SPACE)).into_bytes();
                    let value = if random.one_in(3) {
                        None
                    } else {
                        Some(format!("value-{step}").into_bytes())
                    };
                    let staged = stage(&state, &key, value.as_deref());
                    versions.entry(key).or_default().insert(staged.place().0, value);
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
                        staged.land(&state);
                        in_flight.push(staged);
                    } else {
                        staged.finish(&state);
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
            _read => {
                let key = format!("key-{:04}", random.below(KEY_SPACE)).into_bytes();
                let mark = state.read_point().0;
                let expected = versions
                    .get(&key)
                    .and_then(|history| history.range(..=mark).next_back())
                    .and_then(|(_place, value)| value.clone());
                let found = read(&store, &key);
                let is_newer_now = state.read_point().0 != mark;
                assert!(is_newer_now || found == expected, "seed {seed:?} step {step} key {key:?}");
            },
        }
    }

    for staged in in_flight {
        staged.finish(&state);
    }
    settle(&io);
    let mark = state.read_point().0;
    for (key, history) in &versions {
        let expected = history.range(..=mark).next_back().and_then(|(_place, value)| value.clone());
        assert_eq!(read(&store, key), expected);
    }
    store.close().expect("close");
    assert_eq!(io.open_handle_count(), 0);
    assert_eq!(io.handle_faults(), OperationCount(0));
}
