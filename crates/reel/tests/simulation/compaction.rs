//! Compaction: what no read can reach goes, and nothing else does

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering;

use crate::shape;
use reel::config::{ColumnConfig, KeyShape, StoreConfig, SyncPolicy, VolumeConfig};
use reel::engine::compact_job::{CompactJob, ConfirmPass, RowsProof, is_under_applied_cover};
use reel::segment::summary_orders::is_wholly_dead;
use reel::engine::cue::MOST_CUES;
use reel::engine::engine_sizing::{COMPACT_JOB_BYTES, LANE_JOB_BYTES};
use reel::engine::open_segment::data_path;
use reel::io::job::JobStep;
use reel::engine::read_path::RecordWant;
use reel::engine::reel_store::ReelStore;
use reel::engine::walk_job::WALK_RETRY;
use reel::engine::reserve_job::RESERVE_PERIOD;
use reel::guard::hold;
use reel::io::{InPlace, Io, in_place};
use reel::record::codec::Codec;
use reel::segment::damage_list::DamagedRange;
use reel::segment::segment_tail::SegmentNumber;
use reel::segment::summary::{DeadCount, LostRows, SummaryPaths, SummaryRows, read_summary};
use reel_api::column_id::ColumnId;
use reel_api::store_error::StoreError;
use reel_api::maintenance_report::MaintenanceReport;
use reel_api::units::{ByteCount, Nanoseconds, SegmentCount, VolumeId, WritePlace};
use reel_api::value_list::{ReadBuffers, ValueAnswer, ValueList};
use reel_sim::seeded_random::Seed;
use reel_sim::simulated_disk::OperationCount;
use reel_sim::simulated_io::SimulatedIo;

const ROOT: &str = "/disk/compact";
const COLUMN: ColumnId = ColumnId(1);
const BUDGET: ByteCount = ByteCount(68 * 1024 * 1024);
const TICK: Nanoseconds = Nanoseconds(1_000_000);
const STEPS: u64 = 20_000;
const CRASH_POINTS: u64 = 40;
const LATE_CRASH_POINTS: u64 = 200;
const AFTER_THE_RETRY: Nanoseconds = Nanoseconds(2 * WALK_RETRY.0);
const SYNC_SEEDS: u64 = 64;

type Store = ReelStore<SimulatedIo>;

fn open(io: &SimulatedIo) -> Store {
    let config = StoreConfig {
        memory_budget: BUDGET,
        threads: shape::threads(),
        volumes: vec![VolumeConfig {
            name: "compact".to_string(),
            roots: vec![PathBuf::from(ROOT)],
            sync: SyncPolicy::Never,
            device: Default::default(),
        }],
        columns: vec![ColumnConfig {
            id: COLUMN,
            name: "column".to_string(),
            key_shape: KeyShape::Variable { most: 64 },
            codec: Codec::None,
            volume: VolumeId(0),
            complete_rows: None,
            sorted_rows: None,
            least_value: None,
        }],
    };
    let store = ReelStore::open(io.clone(), &config).expect("open");
    // these segments keep the few dead rows they were built with
    for volume in &store.state.volumes {
        volume.holds.reclaim.store(true, Ordering::Release);
    }
    store
}

fn settle(io: &SimulatedIo) {
    for _step in 0..STEPS {
        if io.step_any().is_none() {
            io.advance_clock(TICK);
            if io.step_any().is_none() {
                return;
            }
        }
    }
    panic!("jobs never went quiet");
}

// put a key and seal it, and return its segment
fn sealed_put(store: &Store, io: &SimulatedIo, value: &[u8]) -> SegmentNumber {
    store.put(COLUMN, b"key", value).expect("put");
    let number = hold(&store.state.volumes[0].claims).open[0].number;
    store.seal_now();
    settle(io);
    number
}

// keep a segment's dead share under half, so a rewrite leaves it alone
fn keep_live(store: &Store) {
    for key in [&b"other"[..], b"other-1", b"other-2"] {
        store.put(COLUMN, key, b"value").expect("put");
    }
}

fn read(store: &Store) -> Option<Vec<u8>> {
    let mut buf = Vec::new();
    store.get(COLUMN, b"key", &mut buf).expect("get").then_some(buf)
}

fn has_file(io: &SimulatedIo, number: SegmentNumber) -> bool {
    let data = data_path(Path::new(ROOT), number).to_path_buf();
    io.list_directory(Path::new(ROOT)).expect("list").contains(&data)
}

// a segment with nothing live leaves the table, the summary and the disk, and the newest value still reads
#[test]
fn dead_unlinks() {
    let io = SimulatedIo::new(Seed(1));
    let store = open(&io);
    let older = sealed_put(&store, &io, b"older");
    let newer = sealed_put(&store, &io, b"newer");

    let volume = &store.state.volumes[0];
    assert!(!volume.table.holds(older));
    assert!(hold(&volume.summary).get(older).is_none());
    assert!(!has_file(&io, older));
    assert!(volume.table.holds(newer));
    assert_eq!(read(&store), Some(b"newer".to_vec()));
    store.close().expect("close");
    assert_eq!(io.open_handle_count(), 0);
}

// a segment with a live row stays
#[test]
fn live_stays() {
    let io = SimulatedIo::new(Seed(2));
    let store = open(&io);
    keep_live(&store);
    let older = sealed_put(&store, &io, b"older");
    sealed_put(&store, &io, b"newer");

    assert!(store.state.volumes[0].table.holds(older));
    assert!(has_file(&io, older));
    store.close().expect("close");
}

// a cue newer than a segment can still need its rows, when their replacements came after the cue, and the segment goes
// as soon as the cue does
#[test]
fn cue_keeps_segment() {
    let io = SimulatedIo::new(Seed(3));
    let store = open(&io);
    let older = sealed_put(&store, &io, b"older");
    let cue = store.cue().expect("cue");
    sealed_put(&store, &io, b"newer");

    let mut buf = Vec::new();
    assert!(store.state.volumes[0].table.holds(older));
    assert!(cue.get(COLUMN, b"key", &mut buf).expect("get"));
    assert_eq!(buf, b"older");

    drop(cue);
    settle(&io);
    assert!(!store.state.volumes[0].table.holds(older));
    store.close().expect("close");
}

// a crash at any step of an unlink loses nothing, the table and the disk agree, and open finds no damage and no lost row
#[test]
fn unlink_crashes() {
    for crash_at in 0..CRASH_POINTS {
        for is_power_cut in [false, true] {
            let io = SimulatedIo::new(Seed(4));
            let store = open(&io);
            store.put(COLUMN, b"key", b"older").expect("put");
            let older = hold(&store.state.volumes[0].claims).open[0].number;
            store.seal_now();
            settle(&io);
            store.put(COLUMN, b"key", b"newer").expect("put");
            store.flush().expect("flush");
            store.seal_now();
            io.arm_crash(OperationCount(crash_at));
            for _step in 0..STEPS {
                if io.is_crashed() {
                    break;
                }
                if io.step_any().is_none() {
                    io.advance_clock(TICK);
                    if io.step_any().is_none() {
                        break;
                    }
                }
            }
            if is_power_cut {
                io.power_cut();
            } else {
                io.kill();
            }
            drop(store);

            let store = open(&io);
            settle(&io);
            assert_eq!(read(&store), Some(b"newer".to_vec()), "crash at {crash_at}");
            assert_eq!(has_file(&io, older), store.state.volumes[0].table.holds(older), "crash at {crash_at}");
            assert!(hold(&store.state.volumes[0].damaged).is_empty(), "crash at {crash_at}");
            assert_eq!(store.state.dropped_summary_rows.load(Ordering::Acquire), 0, "crash at {crash_at}");
            store.close().expect("close");
        }
    }
}

fn hold_compaction(store: &Store) {
    store.state.volumes[0].holds.compaction.store(true, Ordering::Release);
}

fn job(store: &Store) -> CompactJob<SimulatedIo> {
    CompactJob::new(Arc::clone(&store.state), 0)
}

fn dead_rows(store: &Store, number: SegmentNumber) -> u64 {
    let rows = hold(&store.state.volumes[0].summary);
    rows.get(number).expect("a row").cursor().column().expect("a column").dead_rows.0
}

fn saved_rows(io: &SimulatedIo) -> SummaryRows {
    let mut rows = SummaryRows::new(ByteCount(0));
    read_summary(io, &SummaryPaths::new(Path::new(ROOT)), &mut rows, &mut LostRows::default()).expect("read");
    rows
}

// step until the armed crash lands or the jobs go quiet, then end the process
fn crash(io: &SimulatedIo, store: Store, is_power_cut: bool) {
    for _step in 0..STEPS {
        if io.is_crashed() {
            break;
        }
        if io.step_any().is_none() {
            io.advance_clock(TICK);
            if io.step_any().is_none() {
                break;
            }
        }
    }
    if is_power_cut {
        io.power_cut();
    } else {
        io.kill();
    }
    drop(store);
}

// an unlink that lands while a get sits at an older point took what that point could see, so the get starts over
#[test]
fn get_crossed() {
    let io = SimulatedIo::new(Seed(10));
    let store = open(&io);
    let older = sealed_put(&store, &io, b"older");
    let latest = store.state.latest_read(COLUMN, b"key").expect("read");
    sealed_put(&store, &io, b"newer");
    assert!(!store.state.volumes[0].table.holds(older));

    let mut buf = Vec::new();
    assert!(crate::latest::answer(&store, &latest, RecordWant::Whole, &mut buf).is_none());
    assert_eq!(read(&store), Some(b"newer".to_vec()));
    store.close().expect("close");
}

// a contains in the same spot starts over too, and a delete since shows
#[test]
fn contains_crossed() {
    let io = SimulatedIo::new(Seed(11));
    let store = open(&io);
    let older = sealed_put(&store, &io, b"older");
    let latest = store.state.latest_read(COLUMN, b"key").expect("read");
    sealed_put(&store, &io, b"newer");
    assert!(!store.state.volumes[0].table.holds(older));

    assert!(store.state.contains_latest(&latest).is_none());
    assert!(store.contains(COLUMN, b"key").expect("contains"));
    store.close().expect("close");
}

// a get_many with no slot to pin its point starts over when an unlink crosses it
#[test]
fn many_crossed() {
    let io = SimulatedIo::new(Seed(12));
    let store = open(&io);
    let state = &store.state;
    let volume = &state.volumes[0];
    let older = sealed_put(&store, &io, b"older");
    let slots: Vec<usize> = (0..MOST_CUES).map(|_slot| state.cues.take(0, WritePlace(u64::MAX - 1)).expect("a slot")).collect();

    let many = state.many_read(volume).expect("a read");
    assert_eq!(many.pin, None);
    sealed_put(&store, &io, b"newer");
    assert!(!volume.table.holds(older));

    let mut values = ValueList::new();
    let mut buffers = ReadBuffers::default();
    assert!(in_place(state.answer_many(&InPlace, &many, COLUMN, &[b"key"], &mut values, &mut buffers)).is_none());
    for slot in slots {
        state.cues.release(slot);
    }
    store.close().expect("close");
}

// a pinned get_many keeps its version through an unlink that stands aside, and the segment goes once the read is done
#[test]
fn many_pinned() {
    let io = SimulatedIo::new(Seed(13));
    let store = open(&io);
    let state = &store.state;
    let volume = &state.volumes[0];
    let older = sealed_put(&store, &io, b"older");

    let many = state.many_read(volume).expect("a read");
    assert!(many.pin.is_some());
    sealed_put(&store, &io, b"newer");
    assert!(volume.table.holds(older));

    let mut values = ValueList::new();
    let mut buffers = ReadBuffers::default();
    let answer = in_place(state.answer_many(&InPlace, &many, COLUMN, &[b"key"], &mut values, &mut buffers)).expect("an answer");
    answer.expect("read");
    assert_eq!(values.answer(0), Some(ValueAnswer::Found(b"older")));
    settle(&io);
    assert!(!volume.table.holds(older));
    store.close().expect("close");
}

// a cue that read its point before an unlink and took its slot after gives the slot back
#[test]
fn cue_crossed() {
    let io = SimulatedIo::new(Seed(14));
    let store = open(&io);
    let state = &store.state;
    let older = sealed_put(&store, &io, b"older");
    let counts = state.cues.counts();
    let point = state.read_point(&state.volumes[0]);
    sealed_put(&store, &io, b"newer");
    assert!(!state.volumes[0].table.holds(older));

    let slot = state.cues.take(0, point).expect("a slot");
    assert!(!state.pin_holds(slot, counts));
    assert!(state.cues.is_empty());
    store.close().expect("close");
}

// a cue that came after the confirm's last look stops the unlink, and so does a replacement the mark hasn't reached
#[test]
fn unlink_looks_again() {
    let io = SimulatedIo::new(Seed(15));
    let store = open(&io);
    hold_compaction(&store);
    let state = &store.state;
    let volume = &state.volumes[0];
    let older = sealed_put(&store, &io, b"older");
    let below = state.read_point(&state.volumes[0]);
    sealed_put(&store, &io, b"newer");
    let replacement = state.read_point(&state.volumes[0]);
    let job = job(&store);

    let slot = state.cues.take(0, below).expect("a slot");
    job.unlink(volume, older, replacement).expect("unlink");
    assert!(volume.table.holds(older));
    state.cues.release(slot);

    job.unlink(volume, older, WritePlace(replacement.0 + 1)).expect("unlink");
    assert!(volume.table.holds(older));

    job.unlink(volume, older, replacement).expect("unlink");
    release_compaction(&store);
    state.ask_compaction();
    settle(&io);
    assert!(!volume.table.holds(older));
    assert!(!has_file(&io, older));
    store.close().expect("close");
}

// counts that call a live row dead break the summary's one promise, and the rows refuse the unlink
#[test]
fn counts_alone() {
    let io = SimulatedIo::new(Seed(17));
    let store = open(&io);
    keep_live(&store);
    let older = sealed_put(&store, &io, b"older");
    let newer = sealed_put(&store, &io, b"newer");

    let state = &store.state;
    let volume = &state.volumes[0];
    let extra = DeadCount {
        segment: older,
        column: COLUMN,
        block: 0,
        rows: 3,
        graves: 0,
        bytes: ByteCount(0),
        kept_bytes: ByteCount(0),
        replacement: WritePlace(0),
    };
    let mut rows = hold(&volume.summary);
    let blocks = rows.get(newer).expect("a row").facts.block_count;
    rows.apply_walk(newer, blocks, &[extra]);
    assert!(is_wholly_dead(&rows.get(older).expect("a row")));
    drop(rows);
    volume.table.drop_live(older, 0, 3, WritePlace(0));

    state.ask_compaction();
    settle(&io);
    assert_eq!(volume.segments_unlinked.load(Ordering::Acquire), 0);
    assert!(store.contains(COLUMN, b"other").expect("contains"));
    store.close().expect("close");
}

// the proof goes by place: an older version in a finished segment replaces nothing
#[test]
fn newest_stays() {
    let io = SimulatedIo::new(Seed(24));
    let store = open(&io);
    keep_live(&store);
    let older = sealed_put(&store, &io, b"older");
    let newest = sealed_put(&store, &io, b"newest");

    let state = &store.state;
    let volume = &state.volumes[0];
    let extra = DeadCount {
        segment: newest,
        column: COLUMN,
        block: 0,
        rows: 1,
        graves: 0,
        bytes: ByteCount(0),
        kept_bytes: ByteCount(0),
        replacement: WritePlace(0),
    };
    let mut rows = hold(&volume.summary);
    let blocks = rows.get(older).expect("a row").facts.block_count;
    rows.apply_walk(older, blocks, &[extra]);
    assert!(is_wholly_dead(&rows.get(newest).expect("a row")));
    drop(rows);
    volume.table.drop_live(newest, 0, 1, WritePlace(0));

    state.ask_compaction();
    settle(&io);
    assert!(volume.table.holds(newest));
    assert_eq!(read(&store), Some(b"newest".to_vec()));
    store.close().expect("close");
}

// the pick passes over a segment whose counts can't be taken at their word
#[test]
fn pick_refuses() {
    let io = SimulatedIo::new(Seed(19));
    let store = open(&io);
    hold_compaction(&store);
    let older = sealed_put(&store, &io, b"older");
    sealed_put(&store, &io, b"newer");

    let rows = hold(&store.state.volumes[0].summary);
    let row = rows.get(older).expect("a row");
    assert!(is_wholly_dead(&row));
    for refused in [
        |row: &mut reel::segment::summary::SummaryRow<'_>| row.facts.has_covers = true,
        |row: &mut reel::segment::summary::SummaryRow<'_>| row.facts.is_damaged = true,
        |row: &mut reel::segment::summary::SummaryRow<'_>| row.facts.walked_blocks = 0,
    ] {
        let mut changed = row;
        refused(&mut changed);
        assert!(!is_wholly_dead(&changed));
    }
    drop(rows);
    store.close().expect("close");
}

// a walk waits for a segment's seal to end, since its rows still answer through the open slot past the switch
#[test]
fn seal_holds_walk() {
    let io = SimulatedIo::new(Seed(20));
    let store = open(&io);
    let state = &store.state;
    store.put(COLUMN, b"key", b"value").expect("put");
    let number = hold(&state.volumes[0].claims).open[0].number;
    store.seal_now();
    for _step in 0..STEPS {
        if !hold(&state.volumes[0].claims).open.iter().any(|segment| segment.number == number) {
            break;
        }
        if io.step_any().is_none() {
            io.advance_clock(TICK);
        }
    }

    io.hold_jobs("reel-seal", true);
    settle(&io);
    assert_eq!(state.volumes[0].sealing.load(Ordering::Acquire), number.0);
    assert_eq!(state.pending_walks(0), 1);
    io.hold_jobs("reel-seal", false);
    settle(&io);
    assert_eq!(state.pending_walks(0), 0);
    store.close().expect("close");
}

// a crash between a seal's tail and its summary row leaves a segment no count holds, and its forward walk counts the
// older row once
#[test]
fn rebuilt_forward() {
    let mut has_gap = false;
    for crash_at in 0..CRASH_POINTS {
        for is_power_cut in [false, true] {
            let io = SimulatedIo::new(Seed(21));
            let store = open(&io);
            keep_live(&store);
            let older = sealed_put(&store, &io, b"older");
            store.put(COLUMN, b"key", b"newer").expect("put");
            let newer = hold(&store.state.volumes[0].claims).open[0].number;
            store.flush().expect("flush");
            store.seal_now();
            io.arm_crash(OperationCount(crash_at));
            crash(&io, store, is_power_cut);

            let has_row = saved_rows(&io).get(newer).is_some();
            let store = open(&io);
            has_gap |= !has_row && store.state.volumes[0].table.holds(newer);
            store.seal_now();
            settle(&io);
            assert_eq!(read(&store), Some(b"newer".to_vec()), "crash at {crash_at}");
            assert_eq!(dead_rows(&store, older), 1, "crash at {crash_at}");
            store.close().expect("close");
        }
    }
    assert!(has_gap);
}

// a crash after an unlink's save and before its file goes leaves a segment with no row, and the older row stays counted once
#[test]
fn unlinked_once() {
    let mut has_gap = false;
    for crash_at in 0..LATE_CRASH_POINTS {
        for is_power_cut in [false, true] {
            let io = SimulatedIo::new(Seed(22));
            let store = open(&io);
            keep_live(&store);
            let older = sealed_put(&store, &io, b"older");
            let middle = sealed_put(&store, &io, b"middle");
            store.put(COLUMN, b"key", b"newest").expect("put");
            store.flush().expect("flush");
            store.seal_now();
            io.arm_crash(OperationCount(crash_at));
            crash(&io, store, is_power_cut);

            has_gap |= saved_rows(&io).get(middle).is_none() && has_file(&io, middle);
            let store = open(&io);
            store.seal_now();
            settle(&io);
            assert_eq!(read(&store), Some(b"newest".to_vec()), "crash at {crash_at}");
            assert_eq!(dead_rows(&store, older), 1, "crash at {crash_at}");
            store.close().expect("close");
        }
    }
    assert!(has_gap);
}

// a row lost after its walk landed leaves counts that hold that walk, so the rebuilt segment takes the backward walk and
// the older row stays counted once
#[test]
fn lost_row_once() {
    let io = SimulatedIo::new(Seed(25));
    let store = open(&io);
    keep_live(&store);
    let older = sealed_put(&store, &io, b"older");
    let newer = sealed_put(&store, &io, b"newer");
    assert_eq!(dead_rows(&store, older), 1);

    let volume = &store.state.volumes[0];
    hold(&volume.summary).remove(newer);
    store.state.save_summary(volume).expect("save");
    io.kill();
    drop(store);

    let store = open(&io);
    settle(&io);
    assert_eq!(dead_rows(&store, older), 1);
    assert_eq!(read(&store), Some(b"newer".to_vec()));
    store.close().expect("close");
}

// the job's buffers fit their plan, and memory_report names them
#[test]
fn job_sized() {
    let io = SimulatedIo::new(Seed(23));
    let store = open(&io);
    let job = job(&store);
    // a worker's seat has a line of its own in the plan
    for seat in job.seats.iter() {
        assert!(seat.heap_bytes() <= LANE_JOB_BYTES, "a seat holds {} bytes", seat.heap_bytes());
    }
    let seats: u64 = job.seats.iter().map(|seat| seat.heap_bytes()).sum();
    let bytes = job.heap_bytes();
    assert!(bytes.0 - seats <= COMPACT_JOB_BYTES);
    assert_eq!(store.state.memory_report().compaction, bytes);
    store.close().expect("close");
}

// a read holding a dead segment holds nothing else up: the file waits for the pin, and later walks and seals go on
#[test]
fn parked_waits() {
    let io = SimulatedIo::new(Seed(30));
    let store = open(&io);
    hold_compaction(&store);
    let state = &store.state;
    let volume = &state.volumes[0];
    let older = sealed_put(&store, &io, b"older");
    sealed_put(&store, &io, b"newer");

    let pin = volume.table.pin(older).expect("a pin");
    job(&store).unlink(volume, older, state.read_point(&state.volumes[0])).expect("unlink");
    assert!(!volume.table.holds(older));
    assert!(has_file(&io, older));
    assert_eq!(volume.segments_parked.load(Ordering::Acquire), 1);

    release_compaction(&store);
    store.put(COLUMN, b"later", b"value").expect("put");
    store.seal_now();
    run_steps(&io);
    assert_eq!(state.pending_walks(0), 0);
    assert!(has_file(&io, older));

    drop(pin);
    run_steps(&io);
    assert!(!has_file(&io, older));
    assert_eq!(volume.segments_parked.load(Ordering::Acquire), 0);
    store.close().expect("close");
    assert_eq!(io.open_handle_count(), 0);
}

// a full park leaves the next unlink for later, and it goes once a slot frees
#[test]
fn park_full() {
    let io = SimulatedIo::new(Seed(31));
    let store = open(&io);
    hold_compaction(&store);
    let state = &store.state;
    let volume = &state.volumes[0];
    let mut numbers = Vec::new();
    let park = hold(&volume.parked).len();
    for _segment in 0..=park {
        numbers.push(sealed_put(&store, &io, b"value"));
    }
    sealed_put(&store, &io, b"newest");

    let job = job(&store);
    let last = numbers[park];
    let pins: Vec<_> = numbers[..park].iter().map(|number| volume.table.pin(*number).expect("a pin")).collect();
    for number in &numbers[..park] {
        job.unlink(volume, *number, state.read_point(&state.volumes[0])).expect("unlink");
    }
    job.unlink(volume, last, state.read_point(&state.volumes[0])).expect("unlink");
    assert!(volume.table.holds(last));

    drop(pins);
    release_compaction(&store);
    state.ask_compaction();
    settle(&io);
    assert!(!volume.table.holds(last));
    assert_eq!(volume.segments_parked.load(Ordering::Acquire), 0);
    store.close().expect("close");
}

// a close takes the files no read holds
#[test]
fn close_clears_parked() {
    let io = SimulatedIo::new(Seed(32));
    let store = open(&io);
    hold_compaction(&store);
    let volume = &store.state.volumes[0];
    let older = sealed_put(&store, &io, b"older");
    sealed_put(&store, &io, b"newer");

    let pin = volume.table.pin(older).expect("a pin");
    job(&store).unlink(volume, older, store.state.read_point(&store.state.volumes[0])).expect("unlink");
    drop(pin);
    store.close().expect("close");
    assert!(!has_file(&io, older));
    assert_eq!(io.open_handle_count(), 0);
}

// a crash with two files parked is two segments with no row, and open drops both
#[test]
fn parked_crash() {
    for is_power_cut in [false, true] {
        let io = SimulatedIo::new(Seed(33));
        let store = open(&io);
        hold_compaction(&store);
        let state = Arc::clone(&store.state);
        let volume = &state.volumes[0];
        let first = sealed_put(&store, &io, b"first");
        let second = sealed_put(&store, &io, b"second");
        sealed_put(&store, &io, b"third");

        let pins = [volume.table.pin(first).expect("a pin"), volume.table.pin(second).expect("a pin")];
        let job = job(&store);
        job.unlink(volume, first, state.read_point(&state.volumes[0])).expect("unlink");
        job.unlink(volume, second, state.read_point(&state.volumes[0])).expect("unlink");
        assert_eq!(volume.segments_parked.load(Ordering::Acquire), 2);
        assert!(has_file(&io, first) && has_file(&io, second));

        if is_power_cut {
            io.power_cut();
        } else {
            io.kill();
        }
        drop(pins);
        drop(job);
        drop(store);
        drop(state);

        let store = open(&io);
        settle(&io);
        assert!(!has_file(&io, first) && !has_file(&io, second));
        assert_eq!(read(&store), Some(b"third".to_vec()));
        assert!(hold(&store.state.volumes[0].damaged).is_empty());
        assert_eq!(store.state.dropped_summary_rows.load(Ordering::Acquire), 0);
        store.close().expect("close");
    }
}

fn release_compaction(store: &Store) {
    store.state.volumes[0].holds.compaction.store(false, Ordering::Release);
}

// step for a while without asking the jobs to go quiet, since a parked file keeps its job coming back
fn run_steps(io: &SimulatedIo) {
    for _step in 0..STEPS / 4 {
        if io.step_any().is_none() {
            io.advance_clock(TICK);
        }
    }
}

// a save that failed still lists the segment on disk, so its file stays parked and holds every grave back until a save lands
#[test]
fn failed_save_keeps_file() {
    let io = SimulatedIo::new(Seed(34));
    let store = open(&io);
    hold_compaction(&store);
    let state = Arc::clone(&store.state);
    let volume = &state.volumes[0];
    let older = sealed_put(&store, &io, b"older");
    sealed_put(&store, &io, b"newer");

    io.fail_writes(OperationCount(1));
    assert!(job(&store).unlink(volume, older, state.read_point(&state.volumes[0])).is_err());
    assert!(!volume.table.holds(older));
    assert_eq!(volume.segments_unlinked.load(Ordering::Acquire), 0);

    let mut job = job(&store);
    release_compaction(&store);
    io.fail_writes(OperationCount(1));
    assert!(matches!(job.step_unit(Nanoseconds(0)), JobStep::IdleUntil { .. }));
    assert!(has_file(&io, older));
    assert_eq!(volume.segments_parked.load(Ordering::Acquire), 1);

    assert_eq!(job.step_unit(AFTER_THE_RETRY), JobStep::Worked);
    assert!(!has_file(&io, older));
    assert_eq!(volume.segments_parked.load(Ordering::Acquire), 0);
    drop(job);
    io.kill();
    drop(store);
    drop(state);

    let store = open(&io);
    settle(&io);
    assert_eq!(read(&store), Some(b"newer".to_vec()));
    assert!(hold(&store.state.volumes[0].damaged).is_empty());
    assert_eq!(store.state.dropped_summary_rows.load(Ordering::Acquire), 0);
    store.close().expect("close");
}

// a rebuilt segment's backward walk keeps the place of what replaced its rows
#[test]
fn rebuilt_keeps_place() {
    let io = SimulatedIo::new(Seed(35));
    let store = open(&io);
    hold_compaction(&store);
    let older = sealed_put(&store, &io, b"older");
    sealed_put(&store, &io, b"newer");
    let replacement = store.state.read_point(&store.state.volumes[0]);
    let volume = &store.state.volumes[0];
    hold(&volume.summary).remove(older);
    store.state.save_summary(volume).expect("save");
    io.kill();
    drop(store);

    let store = open(&io);
    hold_compaction(&store);
    settle(&io);
    assert_eq!(store.state.volumes[0].table.dead_replacement(older), Some(replacement));
    store.close().expect("close");
}

// the walk keeps the place that replaced a segment's rows, for a whole unlink to check cues against with no lookups
#[test]
fn walk_keeps_place() {
    let io = SimulatedIo::new(Seed(26));
    let store = open(&io);
    hold_compaction(&store);
    store.put(COLUMN, b"key", b"oldest").expect("put");
    let older = sealed_put(&store, &io, b"older");
    sealed_put(&store, &io, b"newer");
    let replacement = store.state.read_point(&store.state.volumes[0]);

    let table = &store.state.volumes[0].table;
    assert_eq!(table.dead_replacement(older), Some(replacement));
    store.close().expect("close");
}

// a segment with a live row has no place to give
#[test]
fn live_has_no_place() {
    let io = SimulatedIo::new(Seed(27));
    let store = open(&io);
    hold_compaction(&store);
    store.put(COLUMN, b"other", b"value").expect("put");
    let older = sealed_put(&store, &io, b"older");
    sealed_put(&store, &io, b"newer");

    assert_eq!(store.state.volumes[0].table.dead_replacement(older), None);
    store.close().expect("close");
}

// the place starts at zero after an open, and that holds nothing back: a cue taken now reads above all that was written
#[test]
fn place_after_open() {
    let io = SimulatedIo::new(Seed(28));
    let store = open(&io);
    hold_compaction(&store);
    let older = sealed_put(&store, &io, b"older");
    sealed_put(&store, &io, b"newer");
    store.close().expect("close");
    drop(store);

    let store = open(&io);
    let volume = &store.state.volumes[0];
    assert_eq!(volume.table.dead_replacement(older), Some(WritePlace(0)));
    let cue = store.cue().expect("cue");
    settle(&io);
    assert!(!volume.table.holds(older));

    let mut buf = Vec::new();
    assert!(cue.get(COLUMN, b"key", &mut buf).expect("get"));
    assert_eq!(buf, b"newer");
    drop(cue);
    store.close().expect("close");
}

const UNLINK_STEPS: u64 = 400;

// a ready unlink proves its segment beside the walks waiting, so ingest that keeps walks waiting still frees segments
#[test]
fn unlink_beside_walks() {
    let io = SimulatedIo::new(Seed(36));
    let store = open(&io);
    hold_compaction(&store);
    let state = &store.state;
    let volume = &state.volumes[0];
    let older = sealed_put(&store, &io, b"older");
    sealed_put(&store, &io, b"newer");

    io.hold_jobs("reel-planner", true);
    for key in [b"later-a", b"later-b"] {
        store.put(COLUMN, key, b"value").expect("put");
        store.seal_now();
        settle(&io);
    }
    assert_eq!(state.pending_walks(0), 2);
    release_compaction(&store);
    state.ask_compaction();
    io.hold_jobs("reel-planner", false);

    for _step in 0..UNLINK_STEPS {
        if !volume.table.holds(older) {
            break;
        }
        assert!(io.step_any().is_some(), "the jobs stopped with the segment still there");
    }
    assert!(!volume.table.holds(older), "the unlink waited behind the walks");
    settle(&io);
    assert_eq!(state.pending_walks(0), 0);
    store.close().expect("close");
}

// a replacement rot took away can't show itself, and the damaged range answers for it: the row is still dead
#[test]
fn rotted_replacement() {
    let io = SimulatedIo::new(Seed(38));
    let store = open(&io);
    hold_compaction(&store);
    let state = &store.state;
    let volume = &state.volumes[0];
    let older = sealed_put(&store, &io, b"older");
    let newer = sealed_put(&store, &io, b"newer");

    state.damage_whole(0, newer);
    for directory in state.directories.iter().flatten() {
        directory.replace(None, &[newer]);
    }
    let retired = volume.table.retire(newer).expect("retired");
    io.close_file(retired.file);
    release_compaction(&store);
    state.ask_compaction();
    settle(&io);
    assert!(!volume.table.holds(older));
    store.close().expect("close");
}

// a replacement in a segment no walk has reached still replaces: a rebuilt row leaves one waiting while the older segment,
// already counted, is ready to go
#[test]
fn replaced_past_the_walk() {
    let io = SimulatedIo::new(Seed(39));
    let store = open(&io);
    hold_compaction(&store);
    let older = sealed_put(&store, &io, b"older");
    let newer = sealed_put(&store, &io, b"newer");
    let volume = &store.state.volumes[0];
    hold(&volume.summary).remove(newer);
    store.state.save_summary(volume).expect("save");
    io.kill();
    drop(store);

    let store = open(&io);
    settle(&io);
    assert!(!store.state.volumes[0].table.holds(older));
    assert_eq!(read(&store), Some(b"newer".to_vec()));
    store.close().expect("close");
}

// an unlink's replacement is durable whatever the sync policy, since a seal syncs its footer: a power cut right after the
// older file goes keeps the newer value
#[test]
fn replacement_survives_power_cut() {
    for seed in 0..SYNC_SEEDS {
        let io = SimulatedIo::new(Seed(seed));
        let store = open(&io);
        let older = sealed_put(&store, &io, b"older");
        let newer = sealed_put(&store, &io, b"newer");
        assert!(!has_file(&io, older));

        io.power_cut();
        drop(store);

        let store = open(&io);
        settle(&io);
        assert!(store.state.volumes[0].table.holds(newer), "seed {seed}");
        assert_eq!(read(&store), Some(b"newer".to_vec()), "seed {seed}");
        store.close().expect("close");
    }
}

// a wrong count charges a block for a row it never lost
fn charge_again(store: &Store, segment: SegmentNumber) {
    let volume = &store.state.volumes[0];
    let count = DeadCount { segment, column: COLUMN, block: 0, rows: 1, replacement: WritePlace(1), ..DeadCount::default() };
    hold(&volume.summary).apply_counts(segment, &[count], None);
    volume.table.drop_live(segment, 0, 1, WritePlace(1));
}

fn sealed_rows(store: &Store, io: &SimulatedIo, keys: &[&[u8]]) -> SegmentNumber {
    for key in keys {
        store.put(COLUMN, key, b"value").expect("put");
    }
    let number = hold(&store.state.volumes[0].claims).open[0].number;
    store.seal_now();
    settle(io);
    number
}

fn read_key(store: &Store, key: &[u8]) -> Option<Vec<u8>> {
    let mut buf = Vec::new();
    store.get(COLUMN, key, &mut buf).expect("get").then_some(buf)
}

fn is_called_dead(store: &Store, segment: SegmentNumber) -> bool {
    let rows = hold(&store.state.volumes[0].summary);
    is_wholly_dead(&rows.get(segment).expect("a row")) && store.state.volumes[0].table.dead_replacement(segment).is_some()
}

fn refused_after_asking(store: &Store, io: &SimulatedIo) -> SegmentCount {
    release_compaction(store);
    store.state.ask_compaction();
    settle(io);
    hold_compaction(store);
    let mut report = MaintenanceReport::new();
    store.maintenance_report(&mut report);
    report.unlinks_refused
}

// counts that call a live row dead unlink nothing: a double charge, then two doubles beside a miss
#[test]
fn unlink_refuses_a_live_row() {
    let io = SimulatedIo::new(Seed(43));
    let store = open(&io);
    hold_compaction(&store);
    let unlinked = store.state.volumes[0].segments_unlinked.load(Ordering::Acquire);

    let doubled = sealed_rows(&store, &io, &[b"a-dead", b"a-live"]);
    sealed_rows(&store, &io, &[b"a-dead"]);
    charge_again(&store, doubled);
    assert!(is_called_dead(&store, doubled));
    assert_eq!(refused_after_asking(&store, &io), SegmentCount(1));
    assert_eq!(read_key(&store, b"a-live"), Some(b"value".to_vec()));

    // a block keeps one total, so three rows counted and one charge more read as two doubles beside a miss
    let missed = sealed_rows(&store, &io, &[b"b-dead-0", b"b-dead-1", b"b-dead-2", b"b-live"]);
    sealed_rows(&store, &io, &[b"b-dead-0", b"b-dead-1", b"b-dead-2"]);
    charge_again(&store, missed);
    assert!(is_called_dead(&store, missed));
    assert_eq!(refused_after_asking(&store, &io), SegmentCount(2));
    assert_eq!(read_key(&store, b"b-live"), Some(b"value".to_vec()));

    assert_eq!(store.state.volumes[0].segments_unlinked.load(Ordering::Acquire), unlinked);
    store.close().expect("close");
}

// rows the counts and the rows both call dead still go, and the report stays at none
#[test]
fn proven_rows_unlink() {
    let io = SimulatedIo::new(Seed(44));
    let store = open(&io);
    hold_compaction(&store);
    let older = sealed_rows(&store, &io, &[b"a", b"b"]);
    sealed_rows(&store, &io, &[b"a", b"b"]);
    assert!(is_called_dead(&store, older));

    assert_eq!(refused_after_asking(&store, &io), SegmentCount(0));
    assert!(!store.state.volumes[0].table.holds(older));
    store.close().expect("close");
}

// an applied cover decides a segment wholly inside it, and one partly outside goes by its rows
#[test]
fn applied_cover_decides_its_unlink() {
    let io = SimulatedIo::new(Seed(45));
    let store = open(&io);
    hold_compaction(&store);
    let volume = &store.state.volumes[0];
    let inside = sealed_rows(&store, &io, &[b"m-0", b"m-1"]);
    let across = sealed_rows(&store, &io, &[b"m-2", b"z-0"]);
    store.delete_range(COLUMN, b"m", b"n").expect("delete");
    store.seal_now();
    settle(&io);

    assert!(is_under_applied_cover(volume, inside));
    assert!(!is_under_applied_cover(volume, across));
    assert_eq!(refused_after_asking(&store, &io), SegmentCount(0));
    assert!(!volume.table.holds(inside));
    assert_eq!(read_key(&store, b"z-0"), Some(b"value".to_vec()));
    store.close().expect("close");
}

// a segment a rewrite claimed stays while the rewrite runs, since its output could keep a row the cover hides
#[test]
fn claimed_segment_waits_for_its_rewrite() {
    let io = SimulatedIo::new(Seed(46));
    let store = open(&io);
    hold_compaction(&store);
    let volume = &store.state.volumes[0];
    let inside = sealed_rows(&store, &io, &[b"m-0", b"m-1"]);
    store.delete_range(COLUMN, b"m", b"n").expect("delete");
    store.seal_now();
    settle(&io);
    assert!(is_under_applied_cover(volume, inside));

    let lane = volume.lanes.len() - 1;
    hold(&volume.lanes[lane].inputs)[0] = inside;
    refused_after_asking(&store, &io);
    assert!(volume.table.holds(inside));

    hold(&volume.lanes[lane].inputs)[0] = SegmentNumber(0);
    refused_after_asking(&store, &io);
    assert!(!volume.table.holds(inside));
    store.close().expect("close");
}

// damage older than a row says nothing about what replaced it, so counts that call that row dead get refused
#[test]
fn old_damage_hides_nothing() {
    let io = SimulatedIo::new(Seed(41));
    let store = open(&io);
    hold_compaction(&store);
    let volume = &store.state.volumes[0];
    let live = sealed_put(&store, &io, b"value");
    volume.damage.add(&DamagedRange {
        column: COLUMN,
        segment: SegmentNumber(u64::MAX),
        highest_place: WritePlace(0),
        start: Vec::new(),
        end: None,
    });

    let pass = ConfirmPass { segment: live, next_block: 0 };
    assert_eq!(job(&store).prove_block(volume, pass).expect("prove"), RowsProof::Live);
    store.close().expect("close");
}

// a ready unlink goes through the yield a writer's index wait puts on the walks, when what it frees matters most
#[test]
fn unlink_through_yield() {
    let io = SimulatedIo::new(Seed(37));
    let store = open(&io);
    hold_compaction(&store);
    let state = &store.state;
    let volume = &state.volumes[0];
    let older = sealed_put(&store, &io, b"older");
    sealed_put(&store, &io, b"newer");

    io.hold_jobs("reel-planner", true);
    store.put(COLUMN, b"later", b"value").expect("put");
    store.seal_now();
    settle(&io);
    state.index_waiters.fetch_add(1, Ordering::AcqRel);
    release_compaction(&store);
    state.ask_compaction();
    io.hold_jobs("reel-planner", false);
    run_steps(&io);
    assert!(!volume.table.holds(older));
    assert_eq!(state.pending_walks(0), 1);
    state.index_waiters.fetch_sub(1, Ordering::AcqRel);
    settle(&io);
    store.close().expect("close");
}

// a seal that needs a reclaim's room gets the walks too, which an index wait would otherwise hold back
#[test]
fn stuck_seal_gets_its_walks() {
    let io = SimulatedIo::new(Seed(42));
    let store = open(&io);
    let state = &store.state;
    let volume = &state.volumes[0];
    io.hold_jobs("reel-planner", true);
    store.put(COLUMN, b"key", b"value").expect("put");
    store.seal_now();
    settle(&io);
    state.index_waiters.fetch_add(1, Ordering::AcqRel);
    state.ask_compaction();
    io.hold_jobs("reel-planner", false);
    run_steps(&io);
    assert_eq!(state.pending_walks(0), 1);

    volume.is_seal_stuck.store(true, Ordering::Release);
    state.ask_compaction();
    run_steps(&io);
    assert_eq!(state.pending_walks(0), 0);
    state.index_waiters.fetch_sub(1, Ordering::AcqRel);
    volume.is_seal_stuck.store(false, Ordering::Release);
    store.close().expect("close");
}

// the walk keeps the highest replacement of a segment's rows: one key's replacement above a cue still needs the segment,
// though the walk meets a lower replacement after it
#[test]
fn cue_between_replacements() {
    let io = SimulatedIo::new(Seed(43));
    let store = open(&io);
    store.put(COLUMN, b"a", b"older").expect("put");
    store.put(COLUMN, b"b", b"older").expect("put");
    let number = hold(&store.state.volumes[0].claims).open[0].number;
    store.seal_now();
    settle(&io);

    store.put(COLUMN, b"b", b"newer").expect("put");
    let cue = store.cue().expect("cue");
    store.put(COLUMN, b"a", b"newer").expect("put");
    store.seal_now();
    settle(&io);

    let mut buf = Vec::new();
    assert!(cue.get(COLUMN, b"a", &mut buf).expect("get"));
    assert_eq!(buf, b"older");

    drop(cue);
    settle(&io);
    assert!(!store.state.volumes[0].table.holds(number));
    store.close().expect("close");
}

// a file's removal that didn't reach the directory stays parked until it has, or a power cut brings the file back after the
// grave over its rows has gone
#[test]
fn parked_until_durable() {
    let io = SimulatedIo::new(Seed(44));
    let store = open(&io);
    hold_compaction(&store);
    let state = Arc::clone(&store.state);
    let volume = &state.volumes[0];
    let older = sealed_put(&store, &io, b"older");
    sealed_put(&store, &io, b"newer");

    let mut job = job(&store);
    job.unlink(volume, older, state.read_point(&state.volumes[0])).expect("unlink");
    release_compaction(&store);
    io.fail_directory_syncs(OperationCount(1));
    assert!(matches!(job.step_unit(Nanoseconds(0)), JobStep::IdleUntil { .. }));
    assert_eq!(volume.segments_parked.load(Ordering::Acquire), 1);

    assert_eq!(job.step_unit(AFTER_THE_RETRY), JobStep::Worked);
    assert_eq!(volume.segments_parked.load(Ordering::Acquire), 0);
    io.power_cut();
    drop(job);
    drop(store);
    drop(state);
    assert!(!has_file(&io, older));
}

// a disk filled with overwritten rows frees itself once writes stop, through merges since every segment keeps a live row
#[test]
fn full_disk_frees_itself() {
    let io = SimulatedIo::new(Seed(48));
    let root = PathBuf::from(ROOT);
    io.add_disk(&root, ByteCount(1 << 40));
    let store = open(&io);
    let volume = &store.state.volumes[0];
    volume.holds.reclaim.store(false, Ordering::Release);
    io.add_disk(&root, ByteCount(8 * volume.segment_bytes.0));
    let value = vec![5; 4096];
    let mut is_full = false;
    'fill: for round in 0..1_000 {
        for number in 0..1_000 {
            let key = if number % 100 == 0 { format!("keep-{round:04}-{number:04}") } else { format!("key-{number:04}") };
            match store.put(COLUMN, key.as_bytes(), &value) {
                Ok(()) => {},
                Err(StoreError::VolumeFull { .. }) => {
                    is_full = true;
                    break 'fill;
                },
                Err(error) => panic!("put failed: {error}"),
            }
        }
    }
    assert!(is_full, "the disk never filled");
    // writes have stopped, so the hot window closes after its quiet spell
    for _round in 0..10 {
        io.advance_clock(Nanoseconds(volume.hot_until().0.saturating_sub(io.clock_reading().0).max(RESERVE_PERIOD.0) + 1));
        settle(&io);
    }
    assert!(volume.segments_rewritten.load(Ordering::Acquire) > 0, "no merge freed the room");
    store.put(COLUMN, b"after", &value).expect("a put once the dead rows went");
    store.close().expect("close");
}

const LIVE_SEGMENTS: u64 = 66;
const LIVE_SEGMENT_KEYS: u64 = 3_000;

// merges that start as segments land show in the report: inputs an earlier merge wrote, and merges started beside another. With
// today's widest-first order a merge takes the previous one's whole-range output, so the first count moves
#[test]
fn overlap_merges_count_their_outputs_taken() {
    let io = SimulatedIo::new(Seed(71));
    let store = open(&io);
    let mut number: u64 = 0;
    for _segment in 0..LIVE_SEGMENTS {
        for _key in 0..LIVE_SEGMENT_KEYS {
            store.put(COLUMN, &number.wrapping_mul(0x9E37_79B9_7F4A_7C15).to_be_bytes(), &[7; 100]).expect("put");
            number += 1;
        }
        store.seal_now();
        settle(&io);
    }
    for _round in 0..20 {
        let until = store.state.quiet_at(io.clock_reading());
        io.advance_clock(Nanoseconds(until.0.saturating_sub(io.clock_reading().0)));
        settle(&io);
    }
    let mut report = MaintenanceReport::default();
    store.maintenance_report(&mut report);
    let volume = &report.volumes[0];
    assert_eq!(volume.overlap_output_inputs, store.state.volumes[0].overlap_output_inputs.load(Ordering::Acquire));
    assert!(volume.overlap_output_inputs > 0, "no merge took an output: {:?}", volume.picks);
    assert!(volume.overlap_inputs > volume.overlap_output_inputs);
    store.close().expect("close");
}

const CROWD_KEYS: u64 = 2_000;
const CROWD_WORKERS: usize = 8;
const CROWD_BUDGET: ByteCount = ByteCount(256 * 1024 * 1024);
const QUIET_ROUNDS: usize = 40;

// a store with a seat a worker, and the overlap bound held at its least
fn open_crowd_config() -> StoreConfig {
    StoreConfig {
        memory_budget: CROWD_BUDGET,
        threads: reel::config::ThreadCounts { maintenance_workers: Some(CROWD_WORKERS), ..reel::config::NO_WORKERS },
        volumes: vec![VolumeConfig {
            name: "crowd".to_string(),
            roots: vec![PathBuf::from(ROOT)],
            sync: SyncPolicy::Never,
            device: Default::default(),
        }],
        columns: vec![ColumnConfig {
            id: COLUMN,
            name: "column".to_string(),
            key_shape: KeyShape::Variable { most: 64 },
            codec: Codec::None,
            volume: VolumeId(0),
            complete_rows: None,
            sorted_rows: None,
            least_value: None,
        }],
    }
}

fn open_crowd(io: &SimulatedIo) -> Store {
    let config = open_crowd_config();
    let store = ReelStore::open(io.clone(), &config).expect("open");
    let volume = &store.state.volumes[0];
    volume.holds.reclaim.store(true, Ordering::Release);
    volume.holds.small_bound.store(true, Ordering::Release);
    store
}

// segments whose keys each spread over the whole range land with the store's own planner held off, the callers go quiet, and one
// hand-out offers every idle seat a pick, as a planner does when a change wakes it in a quiet volume. Give the merges it started
// and the inputs they took
fn crowd_hand_out(io: &SimulatedIo, store: &Store, segments: u64) -> (CompactJob<SimulatedIo>, u64, u64) {
    let volume = &store.state.volumes[0];
    volume.holds.compaction.store(true, Ordering::Release);
    let mut number: u64 = 0;
    for _segment in 0..segments {
        for _key in 0..CROWD_KEYS {
            store.put(COLUMN, &number.wrapping_mul(0x9E37_79B9_7F4A_7C15).to_be_bytes(), &[7; 100]).expect("put");
            number += 1;
        }
        store.seal_now();
        settle(io);
    }
    let until = store.state.quiet_at(io.clock_reading());
    io.advance_clock(Nanoseconds(until.0.saturating_sub(io.clock_reading().0)));
    settle(io);
    volume.holds.compaction.store(false, Ordering::Release);
    let mut job = CompactJob::new(Arc::clone(&store.state), 0);
    let before = volume.picks[reel_api::maintenance_report::PickOutcome::Overlap.index()].load(Ordering::Acquire);
    job.hand_out();
    let merges = volume.picks[reel_api::maintenance_report::PickOutcome::Overlap.index()].load(Ordering::Acquire) - before;
    let inputs = volume.lanes.iter().map(|lane| hold(&lane.inputs).iter().filter(|input| input.0 != 0).count() as u64).sum();
    (job, merges, inputs)
}

// let the merges a hand-out started run to their installs with the callers quiet, and give the segments left and the overlap
// merges the volume ever started
fn finish_merges(io: &SimulatedIo, store: &Store, job: &CompactJob<SimulatedIo>) -> (u64, u64) {
    // no worker steps a seat of a planner the test built, so the test runs each claimed merge itself
    for seat in job.seats.iter() {
        if let Some(rewrite) = hold(&seat.job).as_mut().filter(|rewrite| rewrite.is_claimed()) {
            assert!(rewrite.begin(), "a claimed merge did not begin");
            while rewrite.is_running() {
                rewrite.step();
            }
        }
    }
    for _round in 0..QUIET_ROUNDS {
        let until = store.state.quiet_at(io.clock_reading());
        io.advance_clock(Nanoseconds(until.0.saturating_sub(io.clock_reading().0)));
        settle(io);
    }
    let mut report = MaintenanceReport::default();
    store.maintenance_report(&mut report);
    let volume = &report.volumes[0];
    (volume.segment_count.0, volume.picks[reel_api::maintenance_report::PickOutcome::Overlap.index()])
}

// 36 segments over every key against a bound of 32: one merge of 8 brings the point to 29, so one hand-out starts that one and
// no other. Before a running merge's inputs counted once, every seat after the first still read 36 and the hand-out started 4
// merges over 32 inputs
#[test]
fn parallel_merges_at_one_point() {
    let io = SimulatedIo::new(Seed(72));
    let store = open_crowd(&io);
    let (job, merges, inputs) = crowd_hand_out(&io, &store, 36);
    assert_eq!((merges, inputs), (1, 8));
    assert_eq!(finish_merges(&io, &store, &job), (29, 1), "the store merged past what the bound asks");
    store.close().expect("close");
}

// 60 segments over every key against a bound of 32 need four merges of 8, and one hand-out starts all four beside each other and
// no fifth. Before, it started 8 over 58 inputs
#[test]
fn merges_beside_each_other_when_far_behind() {
    let io = SimulatedIo::new(Seed(73));
    let store = open_crowd(&io);
    let (job, merges, inputs) = crowd_hand_out(&io, &store, 60);
    assert_eq!((merges, inputs), (4, 32));
    assert_eq!(finish_merges(&io, &store, &job), (32, 4), "the store merged past what the bound asks");
    store.close().expect("close");
}

// a merge given up before it began gives its segments back, the counter moves, and the next hand-out finds the point crowded
// again and starts one merge, with no answer kept from while the first held them
#[test]
fn a_given_up_merge_leaves_the_point_crowded() {
    let io = SimulatedIo::new(Seed(74));
    let store = open_crowd(&io);
    let (mut job, merges, inputs) = crowd_hand_out(&io, &store, 36);
    assert_eq!((merges, inputs), (1, 8));
    let volume = &store.state.volumes[0];
    let changes = volume.overlap_changes.load(Ordering::Acquire);
    for seat in job.seats.iter() {
        if let Some(rewrite) = hold(&seat.job).as_mut().filter(|rewrite| rewrite.is_claimed()) {
            rewrite.abandon();
        }
    }
    assert!(volume.overlap_changes.load(Ordering::Acquire) > changes, "a give-up left the counter where it was");
    assert!(!volume.is_overlap_running());
    let overlap = |volume: &reel::engine::volume::Volume<SimulatedIo>| volume.picks[reel_api::maintenance_report::PickOutcome::Overlap.index()].load(Ordering::Acquire);
    let before = overlap(volume);
    job.hand_out();
    assert_eq!(overlap(volume) - before, 1, "the point read clear after the merge over it gave up");
    store.close().expect("close");
}

const SPLIT_BUDGET: ByteCount = ByteCount(80 * 1024 * 1024);
const SPLIT_WORKERS: usize = 2;
const SPLIT_KEYS: u64 = 2_500;
const SPLIT_VALUE_BYTES: usize = 3_000;
const SPLIT_SEGMENTS: u64 = 39;

// a lane a worker has cleared the inputs of and not yet its flag reads as merging with no inputs, groups nothing, and leaves the
// pick as it was
#[test]
fn an_overlap_lane_with_no_inputs_groups_nothing() {
    let io = SimulatedIo::new(Seed(75));
    let store = open_crowd(&io);
    let volume = &store.state.volumes[0];
    // the last lane is no early seat's, so the second seat's pick sees one real merge and this empty lane beside it
    let last = volume.lanes.len() - 1;
    volume.lanes[last].is_overlap.store(true, Ordering::Release);
    let (_job, merges, inputs) = crowd_hand_out(&io, &store, 36);
    assert_eq!((merges, inputs), (1, 8));
    volume.lanes[last].is_overlap.store(false, Ordering::Release);
    store.close().expect("close");
}

// 39 segments large enough that one merge of 8 writes more than one output: counted once the merge brings the point to exactly
// 32, so one count too many would start a second merge on the free seat. While it copies, and after its install with each
// output counted as itself, nothing else starts
#[test]
fn a_merge_counts_once_mid_copy_and_by_its_outputs_after() {
    let io = SimulatedIo::new(Seed(76));
    let config = StoreConfig {
        memory_budget: SPLIT_BUDGET,
        threads: reel::config::ThreadCounts { maintenance_workers: Some(SPLIT_WORKERS), ..reel::config::NO_WORKERS },
        ..open_crowd_config()
    };
    let store = ReelStore::open(io.clone(), &config).expect("open");
    let volume = &store.state.volumes[0];
    volume.holds.reclaim.store(true, Ordering::Release);
    volume.holds.small_bound.store(true, Ordering::Release);
    volume.holds.compaction.store(true, Ordering::Release);
    let value = vec![7; SPLIT_VALUE_BYTES];
    let mut number: u64 = 0;
    for _segment in 0..SPLIT_SEGMENTS {
        for _key in 0..SPLIT_KEYS {
            store.put(COLUMN, &number.wrapping_mul(0x9E37_79B9_7F4A_7C15).to_be_bytes(), &value).expect("put");
            number += 1;
        }
        store.seal_now();
        settle(&io);
    }
    let until = store.state.quiet_at(io.clock_reading());
    io.advance_clock(Nanoseconds(until.0.saturating_sub(io.clock_reading().0)));
    settle(&io);
    volume.holds.compaction.store(false, Ordering::Release);
    let overlap = || volume.picks[reel_api::maintenance_report::PickOutcome::Overlap.index()].load(Ordering::Acquire);
    let mut job = CompactJob::new(Arc::clone(&store.state), 0);
    job.hand_out();
    assert_eq!(overlap(), 1);
    let seats = Arc::clone(&job.seats);
    let seat = seats.iter().find(|seat| hold(&seat.job).as_ref().is_some_and(|rewrite| rewrite.is_claimed())).expect("a claimed seat");
    {
        let mut held = hold(&seat.job);
        let rewrite = held.as_mut().expect("a rewrite");
        assert!(rewrite.begin());
        while rewrite.phase != reel::engine::rewrite_job::RewritePhase::Copying {
            rewrite.step();
        }
        rewrite.step();
        assert!(rewrite.is_running(), "the merge ended before the mid-copy look");
    }
    job.hand_out();
    assert_eq!(overlap(), 1, "a look mid-copy counted the merge's inputs apart");
    {
        let mut held = hold(&seat.job);
        let rewrite = held.as_mut().expect("a rewrite");
        while rewrite.is_running() {
            rewrite.step();
        }
    }
    let mut report = MaintenanceReport::default();
    store.maintenance_report(&mut report);
    let segments = report.volumes[0].segment_count.0;
    assert!(segments >= SPLIT_SEGMENTS - 8 + 2, "the merge wrote one output, so nothing split: {segments}");
    job.hand_out();
    assert_eq!(overlap(), 1, "a look after the install started a merge");
    store.close().expect("close");
}

// 77 segments over every key against a bound of 32, as the 50M load left the box: one hand-out starts merges while the count
// net of running merges is over the bound, so 7 merges of 8 bring it to 28, and no eighth
#[test]
fn far_behind_starts_what_the_count_asks() {
    let io = SimulatedIo::new(Seed(77));
    let store = open_crowd(&io);
    let (job, merges, inputs) = crowd_hand_out(&io, &store, 77);
    assert_eq!((merges, inputs), (7, 56));
    assert_eq!(finish_merges(&io, &store, &job), (28, 7), "the store merged past what the bound asks");
    store.close().expect("close");
}

// the same, with five more segments sealing while the 7 merges run, as the box's settle sealed the load's open tail: 28 and 5
// is 33, one over, so one more merge starts and the store ends under the bound
#[test]
fn seals_during_far_behind_merges_start_one_more() {
    let io = SimulatedIo::new(Seed(78));
    let store = open_crowd(&io);
    let (job, merges, inputs) = crowd_hand_out(&io, &store, 77);
    assert_eq!((merges, inputs), (7, 56));
    let mut number: u64 = 1 << 40;
    for _segment in 0..5 {
        for _key in 0..CROWD_KEYS {
            store.put(COLUMN, &number.wrapping_mul(0x9E37_79B9_7F4A_7C15).to_be_bytes(), &[7; 100]).expect("put");
            number += 1;
        }
        store.seal_now();
        settle(&io);
    }
    let (segments, ever) = finish_merges(&io, &store, &job);
    assert_eq!(ever, 8, "five seals over the bound started other than one merge");
    assert!(segments <= 32, "{segments} segments over every key after the merges");
    store.close().expect("close");
}

const FINISH_WALL: std::time::Duration = std::time::Duration::from_secs(30);

// run a finish on its own thread while this one steps the simulated jobs, and return what it said
fn finish_stepped(store: &Store, io: &SimulatedIo) -> Result<(), StoreError> {
    std::thread::scope(|scope| {
        let finishing = scope.spawn(|| store.finish());
        // the finish sleeps on the real clock between looks, so the jobs are stepped until it answers or the wall time runs out
        let deadline = std::time::Instant::now() + FINISH_WALL;
        while std::time::Instant::now() < deadline {
            if finishing.is_finished() {
                break;
            }
            if io.step_any().is_none() {
                io.advance_clock(TICK);
            }
        }
        assert!(finishing.is_finished(), "the finish never answered");
        finishing.join().expect("the finish thread")
    })
}

// a finish on a full disk answers with the error and leaves the store open, and once room comes back a second finish seals
// everything, and the store reads every row after a reopen
#[test]
fn finish_on_a_full_disk_answers_and_leaves_the_store_open() {
    let io = SimulatedIo::new(Seed(201));
    let store = open(&io);
    for number in 0..100_u32 {
        store.put(COLUMN, format!("key-{number:03}").as_bytes(), b"value").expect("put");
    }
    io.fill_writes(OperationCount(u64::MAX));
    let refused = finish_stepped(&store, &io);
    assert!(
        matches!(&refused, Err(StoreError::Io { source, .. }) if source.kind() == std::io::ErrorKind::StorageFull),
        "{refused:?}"
    );
    assert!(store.check_open().is_ok(), "the store closed on a failed finish");
    io.fill_writes(OperationCount(0));
    finish_stepped(&store, &io).expect("a finish once room came back");
    drop(store);
    let store = open(&io);
    let mut value = Vec::new();
    for number in 0..100_u32 {
        assert!(store.get(COLUMN, format!("key-{number:03}").as_bytes(), &mut value).expect("get"), "key {number}");
    }
    store.close().expect("close");
}
