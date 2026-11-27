//! The scrub: a paced pass over the sealed segments that checks every record and marks a rotted one as damage

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use reel::config::{ColumnConfig, ColumnIndex, KeyShape, NO_WORKERS, StoreConfig, SyncPolicy, ThreadCounts, VolumeConfig};
use reel::engine::compact_job::CompactJob;
use reel::engine::open_segment::data_path;
use reel::engine::reel_store::ReelStore;
use reel::engine::scrub_job::{SCRUB_PASS_REST, STEP_BYTES, ScrubJob};
use reel::engine::volume::NO_REBUILD;
use reel::engine::walk_job::FREE_IN_PLACE_BYTES;
use reel::guard::hold;
use reel::io::job::JobStep;
use reel::io::{InPlace, Io, in_place};
use reel::record::codec::Codec;
use reel::record::record_head::{MOST_SUMMED_PAYLOAD, PAYLOAD_SUM_AT, SMALL_HEAD_BYTES};
use reel::superblock::{OLDER_SUPERBLOCK_MAGIC, SECOND_SUPERBLOCK_NAME, SUPERBLOCK_MAGIC, SUPERBLOCK_NAME};
use reel::segment::damage_list::DamagedRange;
use reel_core::maintenance_report::PickOutcome;
use reel::segment::segment_tail::SegmentNumber;
use reel_core::column_id::ColumnId;
use reel_core::direction::Direction;
use reel_core::scan::ScanStep;
use reel_core::value_list::{ValueAnswer, ValueList};
use reel_core::store_error::StoreError;
use reel_core::units::{ByteCount, FileOffset, Nanoseconds, RowCount, VolumeId, WritePlace};
use reel_sim::file_image::BitMask;
use reel_sim::simulated_disk::{DirectReads, OperationCount};
use reel_sim::seeded_random::{Seed, SeededRandom};
use reel_sim::simulated_io::SimulatedIo;

const ROOT: &str = "/disk/scrub";
const COLUMN: ColumnId = ColumnId(1);
const BUDGET: ByteCount = ByteCount(64 * 1024 * 1024);
const TICK: Nanoseconds = Nanoseconds(1_000_000);
const STEPS: u64 = 40_000;
const KEYS: u64 = 300;
const RATE: u64 = 1024 * 1024;
const MOST_SCRUB_STEPS: u64 = 100_000;
const BUSY_ROUNDS: u64 = 12;
const BUSY_KEYS: u64 = 40;

type Store = ReelStore<SimulatedIo>;

fn open(io: &SimulatedIo, scrub_bytes_per_second: u64) -> Store {
    let config = StoreConfig {
        memory_budget: BUDGET,
        max_memory: None,
        threads: ThreadCounts { scrub_bytes_per_second, ..NO_WORKERS },
        volumes: vec![VolumeConfig {
            name: "scrub".to_string(),
            roots: vec![PathBuf::from(ROOT)],
            sync: SyncPolicy::Never,
            device: Default::default(), open_segments: None, segment_bytes: None, lifetime_groups: None,
        }],
        columns: vec![ColumnConfig {
            id: COLUMN,
            name: "column".to_string(),
            key_shape: KeyShape::Variable { most: 64 },
            codec: Codec::None,
            volume: VolumeId(0),
            index: ColumnIndex::Plain,
            least_rows: None,
            least_value: None,
        }],
    };
    ReelStore::open(io.clone(), &config).expect("open")
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

fn seal(store: &Store, io: &SimulatedIo) -> SegmentNumber {
    let number = hold(&store.state.volumes[0].claims).open[0].number;
    store.seal_now();
    settle(io);
    number
}

fn key(number: u64) -> Vec<u8> {
    format!("key-{number:05}").into_bytes()
}

fn value(number: u64, round: u64) -> Vec<u8> {
    format!("value-{number:05}-{round}").into_bytes()
}

fn read(store: &Store, name: &[u8]) -> Option<Vec<u8>> {
    let mut buf = Vec::new();
    store.get(COLUMN, name, &mut buf).expect("get").then_some(buf)
}

// step a scrub of its own until it ends a pass, with the clock moving while the rate holds it back, and say how many steps
fn scrub_one_pass(store: &Store, io: &SimulatedIo, job: &mut ScrubJob<SimulatedIo>) -> u64 {
    let passes = store.state.scrub_passes.load(Ordering::Relaxed);
    for step in 0..MOST_SCRUB_STEPS {
        if store.state.scrub_passes.load(Ordering::Relaxed) > passes {
            return step;
        }
        match job.step_unit(io.now()) {
            JobStep::Worked => {},
            JobStep::IdleUntil { deadline } => io.advance_clock(Nanoseconds(deadline.0.saturating_sub(io.now().0).max(1))),
            JobStep::Idle | JobStep::Finished => return step,
        }
    }
    panic!("the scrub never ended a pass");
}

// the scrub holds the key of the record it finds rotten, so the damage is that key alone: every other key of the column reads, the
// rotten one refuses, its segment is still listed for a rebuild, and a reopen keeps it so
#[test]
fn a_rotten_record_the_scrub_finds_damages_its_key_alone() {
    let io = SimulatedIo::new(Seed(61));
    let store = open(&io, 0);
    let (segment, name) = rot_and_scrub(&store, &io);
    let check = |store: &Store| {
        let mut buf = Vec::new();
        assert!(matches!(store.get(COLUMN, &name, &mut buf), Err(StoreError::Corruption { .. })), "the rotten key read");
        for number in (0..KEYS).filter(|number| key(*number) != name) {
            assert_eq!(read(store, &key(number)), Some(value(number, 0)), "key {number} refused beside one rotten key");
        }
        assert_eq!(store.damaged_segment_list(), vec![(VolumeId(0), segment)]);
    };
    check(&store);
    store.close().expect("close");
    drop(store);
    let store = open(&io, 0);
    check(&store);
    store.close().expect("close");
}

// a flipped byte in a sealed record is found within one pass: its segment is damaged, and the counters show the failure and the
// bytes checked
#[test]
fn a_flipped_byte_is_found_within_one_pass() {
    let io = SimulatedIo::new(Seed(1));
    let store = open(&io, 0);
    for number in 0..KEYS {
        store.put(COLUMN, &key(number), &value(number, 0)).expect("put");
    }
    let segment = seal(&store, &io);
    let volume = &store.state.volumes[0];
    let name = key(KEYS / 2);
    let read_at = store.state.key_read(COLUMN, &name, store.state.read_point(volume), false).expect("read");
    let row = in_place(store.state.search_sealed(&InPlace, volume, &read_at)).expect("search").expect("a sealed row");
    io.rot(&data_path(&volume.roots[0], row.segment), FileOffset(row.row.offset.0 + SMALL_HEAD_BYTES as u64), BitMask(1)).expect("rot");

    let mut job = ScrubJob::new(Arc::clone(&store.state), 0);
    job.rate = RATE;
    scrub_one_pass(&store, &io, &mut job);
    assert_eq!(store.state.scrub_failures.load(Ordering::Relaxed), 1);
    assert!(hold(&volume.damaged).contains(&segment), "the scrub left the rotted segment undamaged");
    assert!(store.state.scrub_bytes.load(Ordering::Relaxed) > 0);
    store.close().expect("close");
}

// a rate of 0 checks nothing
#[test]
fn rate_zero_runs_nothing() {
    let io = SimulatedIo::new(Seed(2));
    let store = open(&io, 0);
    for number in 0..KEYS {
        store.put(COLUMN, &key(number), &value(number, 0)).expect("put");
    }
    seal(&store, &io);
    let mut job = ScrubJob::new(Arc::clone(&store.state), 0);
    assert_eq!(job.step_unit(io.now()), JobStep::Idle);
    settle(&io);
    assert_eq!(store.state.scrub_bytes.load(Ordering::Relaxed), 0);
    assert_eq!(store.state.scrub_passes.load(Ordering::Relaxed), 0);
    store.close().expect("close");
}

// a segment a merge takes away while a pass stands in it is passed over with no error and no damage
#[test]
fn a_segment_leaving_mid_pass_is_passed_over() {
    let io = SimulatedIo::new(Seed(3));
    let store = open(&io, 0);
    for number in 0..KEYS {
        store.put(COLUMN, &key(number), &value(number, 0)).expect("put");
    }
    // half the keys again, so a merge of the segment drops rows and makes a new one
    for number in (0..KEYS).filter(|number| number % 2 == 0) {
        store.put(COLUMN, &key(number), &value(number, 1)).expect("put");
    }
    let first = seal(&store, &io);
    let mut job = ScrubJob::new(Arc::clone(&store.state), 0);
    job.rate = RATE;
    assert_eq!(job.step_unit(io.now()), JobStep::Worked);
    let mut merge = CompactJob::new(Arc::clone(&store.state), 0);
    merge.rewrite.forced = Some(first);
    if merge.rewrite.start() {
        while merge.rewrite.is_running() {
            merge.rewrite.step();
        }
    }
    settle(&io);
    assert!(!store.state.volumes[0].table.holds(first), "the merge left its input in place");
    scrub_one_pass(&store, &io, &mut job);
    assert_eq!(store.state.scrub_failures.load(Ordering::Relaxed), 0);
    assert!(hold(&store.state.volumes[0].damaged).is_empty());
    for number in 0..KEYS {
        assert_eq!(read(&store, &key(number)), Some(value(number, u64::from(number % 2 == 0))));
    }
    store.close().expect("close");
}

// the store's own scrub runs beside writers, seals, merges and frees in place, and finds no false damage
#[test]
fn a_scrub_beside_writers_and_merges_finds_no_false_damage() {
    let io = SimulatedIo::new(Seed(4));
    let store = open(&io, RATE);
    let mut random = SeededRandom::new(Seed(4));
    let large = vec![7_u8; FREE_IN_PLACE_BYTES.0 as usize];
    for round in 0..BUSY_ROUNDS {
        for number in 0..BUSY_KEYS {
            match random.below(4) {
                0 => store.put(COLUMN, &key(number), &large).expect("put"),
                1 => store.delete(COLUMN, &key(number)).expect("delete"),
                _ => store.put(COLUMN, &key(number), &value(number, round)).expect("put"),
            }
        }
        seal(&store, &io);
        io.advance_clock(Nanoseconds(TICK.0 * 1_000));
        settle(&io);
    }
    assert!(store.state.scrub_bytes.load(Ordering::Relaxed) > 0, "the store's scrub checked nothing");
    assert_eq!(store.state.scrub_failures.load(Ordering::Relaxed), 0);
    assert!(hold(&store.state.volumes[0].damaged).is_empty(), "the scrub damaged a sound segment");
    store.close().expect("close");
}

// a value read under trust and held in its index leaf stops answering once the scrub marks its record bad: the next get refuses.
// The flipped byte sits in the stored check, which a trusted get skips, so the get holds the value whole
#[test]
fn a_held_value_stops_answering_after_the_scrub() {
    let io = SimulatedIo::new(Seed(5));
    let mut config = sorted_config();
    config.threads = ThreadCounts { scrub_bytes_per_second: 0, ..NO_WORKERS };
    let store = ReelStore::open(io.clone(), &config).expect("open");
    for number in 0..KEYS {
        store.put(SORTED, &sorted_key(number), &value(number, 0)).expect("put");
    }
    seal(&store, &io);
    let volume = &store.state.volumes[0];
    let name = sorted_key(KEYS / 2);
    let read_at = store.state.key_read(SORTED, &name, store.state.read_point(volume), false).expect("read");
    let row = in_place(store.state.search_sealed(&InPlace, volume, &read_at)).expect("search").expect("a sealed row");
    io.rot(&data_path(&volume.roots[0], row.segment), FileOffset(row.row.offset.0), BitMask(1)).expect("rot");
    let mut buf = Vec::new();
    // the get trusts the sealed record its counts vouch for, skips its identity and holds the value
    assert!(store.get(SORTED, &name, &mut buf).expect("get"));
    assert_eq!(buf, value(KEYS / 2, 0), "the get checked the identity");
    assert!(store.state.sorted_index(SORTED).expect("an index").values.held.load(Ordering::Relaxed) > 0, "nothing was held");

    let mut job = ScrubJob::new(Arc::clone(&store.state), 0);
    job.rate = RATE;
    scrub_one_pass(&store, &io, &mut job);
    assert_eq!(store.state.scrub_failures.load(Ordering::Relaxed), 1);
    buf.clear();
    let again = store.get(SORTED, &name, &mut buf);
    assert!(matches!(again, Err(StoreError::Corruption { .. })), "the held value answered after the scrub: {again:?}");
    store.close().expect("close");
}

const SORTED: ColumnId = ColumnId(2);
const FULL_BUDGET_BYTES: ByteCount = ByteCount(1024 * 1024 * 1024);

fn sorted_key(number: u64) -> Vec<u8> {
    let mut key = [0_u8; 16];
    key[8..].copy_from_slice(&number.to_be_bytes());
    key.to_vec()
}

// a store on a full budget with a sorted column, so its index leaves hold values
fn sorted_config() -> StoreConfig {
    StoreConfig {
        memory_budget: FULL_BUDGET_BYTES,
        max_memory: None,
        threads: NO_WORKERS,
        volumes: vec![VolumeConfig {
            name: "scrub".to_string(),
            roots: vec![PathBuf::from(ROOT)],
            sync: SyncPolicy::Never,
            device: Default::default(), open_segments: None, segment_bytes: None, lifetime_groups: None,
        }],
        columns: vec![ColumnConfig {
            id: SORTED,
            name: "sorted".to_string(),
            key_shape: KeyShape::Fixed { width: 16 },
            codec: Codec::None,
            volume: VolumeId(0),
            index: ColumnIndex::Sorted,
            least_rows: None,
            least_value: None,
        }],
    }
}

// a writable store's scan checks every record while its volume has damage marked anywhere, so it steps corrupt at a rotten key
// and goes on to the next. The damage here lies below every key the scan reads, so the walk itself never meets it
#[test]
fn a_scan_checks_while_damage_is_marked() {
    let io = SimulatedIo::new(Seed(6));
    let store = open(&io, 0);
    for number in 0..KEYS {
        store.put(COLUMN, &key(number), &value(number, 0)).expect("put");
    }
    seal(&store, &io);
    let volume = &store.state.volumes[0];
    let name = key(KEYS / 2);
    let read_at = store.state.key_read(COLUMN, &name, store.state.read_point(volume), false).expect("read");
    let row = in_place(store.state.search_sealed(&InPlace, volume, &read_at)).expect("search").expect("a sealed row");
    io.rot(&data_path(&volume.roots[0], row.segment), FileOffset(row.row.offset.0 + SMALL_HEAD_BYTES as u64), BitMask(1)).expect("rot");
    volume.damage.add(&DamagedRange { column: COLUMN, segment: row.segment, highest_place: WritePlace(0), start: Vec::new(), end: Some(vec![0]) });

    let mut scan = store.iter_from(COLUMN, &key(0), Direction::Ascending).expect("scan");
    let (mut name_buf, mut value_buf) = (Vec::new(), Vec::new());
    let (mut corrupt, mut after) = (Vec::new(), 0);
    loop {
        match scan.next_row(&mut name_buf, &mut value_buf).expect("step") {
            ScanStep::Found => {
                assert_ne!(name_buf, name, "the rotten key answered a value");
                after += u64::from(!corrupt.is_empty());
            },
            ScanStep::Corrupt => corrupt.push(name_buf.clone()),
            ScanStep::End => break,
        }
    }
    assert_eq!(corrupt, vec![name], "the scan did not step corrupt at the rotten key alone");
    assert!(after > 0, "the scan stopped at the corrupt row");
    drop(scan);
    store.close().expect("close");
}

// a value a trusted get read past its flipped check and the record cache took stops answering once damage is marked on its volume: a get and
// a batched get both read the record past the cache and check it. A plain column's get tries its spot first, so this pins the
// spot path's guard as well as the sealed get's
#[test]
fn gets_check_past_the_cache_while_damage_is_marked() {
    let io = SimulatedIo::new(Seed(7));
    let store = open(&io, 0);
    for number in 0..KEYS {
        store.put(COLUMN, &key(number), &value(number, 0)).expect("put");
    }
    seal(&store, &io);
    let volume = &store.state.volumes[0];
    let name = key(KEYS / 2);
    let read_at = store.state.key_read(COLUMN, &name, store.state.read_point(volume), false).expect("read");
    let row = in_place(store.state.search_sealed(&InPlace, volume, &read_at)).expect("search").expect("a sealed row");
    io.rot(&data_path(&volume.roots[0], row.segment), FileOffset(row.row.offset.0), BitMask(1)).expect("rot");
    // the trusted gets skip the identity, and the second miss puts the value in the cache
    let mut buf = Vec::new();
    for _round in 0..3 {
        buf.clear();
        assert!(store.get(COLUMN, &name, &mut buf).expect("get"));
    }
    assert_eq!(buf, value(KEYS / 2, 0), "the trusted get checked the identity");
    volume.damage.add(&DamagedRange { column: COLUMN, segment: row.segment, highest_place: WritePlace(0), start: Vec::new(), end: Some(vec![0]) });

    buf.clear();
    let again = store.get(COLUMN, &name, &mut buf);
    assert!(matches!(again, Err(StoreError::Corruption { .. })), "a get answered past the damage: {again:?}");
    let mut values = ValueList::new();
    let names: Vec<Vec<u8>> = (KEYS / 2 - 4..KEYS / 2 + 4).map(key).collect();
    let keys: Vec<&[u8]> = names.iter().map(Vec::as_slice).collect();
    let batched = store.get_many(COLUMN, &keys, &mut values);
    let answer = batched.map(|()| values.answer(4));
    assert!(matches!(answer, Ok(Some(ValueAnswer::Corrupt)) | Err(StoreError::Corruption { .. })), "a batched get answered past the damage: {answer:?}");
    store.close().expect("close");
}

// on a column with a complete table, a value trusted scans read past its flipped check and left in the record cache stops
// answering a get once damage is marked on its volume
#[test]
fn a_scanned_value_stops_answering_once_damage_is_marked() {
    let io = SimulatedIo::new(Seed(8));
    let mut config = StoreConfig {
        memory_budget: FULL_BUDGET_BYTES,
        max_memory: None,
        threads: NO_WORKERS,
        volumes: vec![VolumeConfig {
            name: "scrub".to_string(),
            roots: vec![PathBuf::from(ROOT)],
            sync: SyncPolicy::Never,
            device: Default::default(), open_segments: None, segment_bytes: None, lifetime_groups: None,
        }],
        columns: Vec::new(),
    };
    config.threads.scrub_bytes_per_second = 0;
    config.columns.push(ColumnConfig {
        id: COLUMN,
        name: "column".to_string(),
        key_shape: KeyShape::Variable { most: 64 },
        codec: Codec::None,
        volume: VolumeId(0),
        index: ColumnIndex::Complete,
        least_rows: Some(RowCount(KEYS * 4)),
        least_value: None,
    });
    let store = ReelStore::open(io.clone(), &config).expect("open");
    for number in 0..KEYS {
        store.put(COLUMN, &key(number), &value(number, 0)).expect("put");
    }
    seal(&store, &io);
    let volume = &store.state.volumes[0];
    let name = key(KEYS / 2);
    let read_at = store.state.key_read(COLUMN, &name, store.state.read_point(volume), false).expect("read");
    let row = in_place(store.state.search_sealed(&InPlace, volume, &read_at)).expect("search").expect("a sealed row");
    io.rot(&data_path(&volume.roots[0], row.segment), FileOffset(row.row.offset.0), BitMask(1)).expect("rot");
    // trusted scans skip the identity and leave the value in the cache
    for _round in 0..3 {
        let mut scan = store.iter_from(COLUMN, b"", Direction::Ascending).expect("scan");
        let (mut name_buf, mut value_buf) = (Vec::new(), Vec::new());
        while scan.next_row(&mut name_buf, &mut value_buf).expect("step") != ScanStep::End {}
    }
    let mut buf = Vec::new();
    let before = store.get(COLUMN, &name, &mut buf);
    assert!(matches!(before, Ok(true)) && buf == value(KEYS / 2, 0), "the scans left nothing to serve: {before:?}");
    volume.damage.add(&DamagedRange { column: COLUMN, segment: row.segment, highest_place: WritePlace(0), start: Vec::new(), end: Some(vec![0]) });
    buf.clear();
    let after = store.get(COLUMN, &name, &mut buf);
    assert!(matches!(after, Err(StoreError::Corruption { .. })), "a get answered the cached value past the damage: {after:?}");
    store.close().expect("close");
}

// rot in a record's stored payload sum is found both ways: a trusted get refuses the record, since the sum no longer matches its
// payload, and the scrub fails it, since the full check compares the stored bytes too
#[test]
fn a_flipped_payload_sum_is_found() {
    let io = SimulatedIo::new(Seed(9));
    let store = open(&io, 0);
    for number in 0..KEYS {
        store.put(COLUMN, &key(number), &value(number, 0)).expect("put");
    }
    let segment = seal(&store, &io);
    let volume = &store.state.volumes[0];
    let name = key(KEYS / 2);
    let read_at = store.state.key_read(COLUMN, &name, store.state.read_point(volume), false).expect("read");
    let row = in_place(store.state.search_sealed(&InPlace, volume, &read_at)).expect("search").expect("a sealed row");
    io.rot(&data_path(&volume.roots[0], row.segment), FileOffset(row.row.offset.0 + PAYLOAD_SUM_AT as u64), BitMask(1)).expect("rot");
    let mut buf = Vec::new();
    let found = store.get(COLUMN, &name, &mut buf);
    assert!(matches!(found, Err(StoreError::Corruption { .. })), "a flipped payload sum answered: {found:?}");

    let mut job = ScrubJob::new(Arc::clone(&store.state), 0);
    job.rate = RATE;
    scrub_one_pass(&store, &io, &mut job);
    assert_eq!(store.state.scrub_failures.load(Ordering::Relaxed), 1);
    assert!(hold(&volume.damaged).contains(&segment), "the scrub passed a record whose stored sum is wrong");
    store.close().expect("close");
}

// a store written before records kept their payload sum is refused at open with an error that says so, and nothing in it changes
#[test]
fn an_older_store_is_refused() {
    let io = SimulatedIo::new(Seed(10));
    let store = open(&io, 0);
    store.put(COLUMN, &key(1), &value(1, 0)).expect("put");
    seal(&store, &io);
    store.close().expect("close");
    drop(store);
    let older = u64::from(OLDER_SUPERBLOCK_MAGIC).to_le_bytes();
    let newer = u64::from(SUPERBLOCK_MAGIC).to_le_bytes();
    for name in [SUPERBLOCK_NAME, SECOND_SUPERBLOCK_NAME] {
        let path = PathBuf::from(ROOT).join(name);
        for (at, (old, new)) in older.iter().zip(&newer).enumerate() {
            if old != new {
                io.rot(&path, FileOffset(at as u64), BitMask(old ^ new)).expect("rot");
            }
        }
    }
    let config = StoreConfig {
        memory_budget: BUDGET,
        max_memory: None,
        threads: ThreadCounts { scrub_bytes_per_second: 0, ..NO_WORKERS },
        volumes: vec![VolumeConfig {
            name: "scrub".to_string(),
            roots: vec![PathBuf::from(ROOT)],
            sync: SyncPolicy::Never,
            device: Default::default(), open_segments: None, segment_bytes: None, lifetime_groups: None,
        }],
        columns: vec![ColumnConfig {
            id: COLUMN,
            name: "column".to_string(),
            key_shape: KeyShape::Variable { most: 64 },
            codec: Codec::None,
            volume: VolumeId(0),
            index: ColumnIndex::Plain,
            least_rows: None,
            least_value: None,
        }],
    };
    let opened = ReelStore::open(io.clone(), &config);
    let error = opened.err().expect("an older store opened");
    assert!(format!("{error:?}").contains("payload sum"), "the refusal doesn't say why: {error:?}");
}

// a trusted get sums a payload of exactly MOST_SUMMED_PAYLOAD bytes and refuses it flipped, and passes one a byte longer
// unsummed, flipped byte and all
#[test]
fn the_summed_payload_line_holds() {
    let io = SimulatedIo::new(Seed(11));
    let store = open(&io, 0);
    let (at_line, past_line) = (vec![b'a'; MOST_SUMMED_PAYLOAD], vec![b'b'; MOST_SUMMED_PAYLOAD + 1]);
    store.put(COLUMN, b"at-line", &at_line).expect("put");
    store.put(COLUMN, b"past-line", &past_line).expect("put");
    seal(&store, &io);
    let volume = &store.state.volumes[0];
    for name in [&b"at-line"[..], b"past-line"] {
        let read_at = store.state.key_read(COLUMN, name, store.state.read_point(volume), false).expect("read");
        let row = in_place(store.state.search_sealed(&InPlace, volume, &read_at)).expect("search").expect("a sealed row");
        io.rot(&data_path(&volume.roots[0], row.segment), FileOffset(row.row.offset.0 + SMALL_HEAD_BYTES as u64 + 7), BitMask(1)).expect("rot");
    }
    let mut buf = Vec::new();
    let at = store.get(COLUMN, b"at-line", &mut buf);
    assert!(matches!(at, Err(StoreError::Corruption { .. })), "a flipped payload at the line answered: {at:?}");
    buf.clear();
    assert!(store.get(COLUMN, b"past-line", &mut buf).expect("get"), "the payload past the line went missing");
    assert_eq!(buf.len(), past_line.len());
    assert_ne!(buf, past_line, "a payload past the line was summed");
    store.close().expect("close");
}

// a compressed record's sum covers its stored bytes, so a trusted get refuses a flipped stored byte before it restores the value
#[test]
fn a_flipped_compressed_payload_is_caught_under_trust() {
    let io = SimulatedIo::new(Seed(12));
    let config = StoreConfig {
        memory_budget: FULL_BUDGET_BYTES,
        max_memory: None,
        threads: ThreadCounts { scrub_bytes_per_second: 0, ..NO_WORKERS },
        volumes: vec![VolumeConfig {
            name: "scrub".to_string(),
            roots: vec![PathBuf::from(ROOT)],
            sync: SyncPolicy::Never,
            device: Default::default(), open_segments: None, segment_bytes: None, lifetime_groups: None,
        }],
        columns: vec![ColumnConfig {
            id: COLUMN,
            name: "column".to_string(),
            key_shape: KeyShape::Variable { most: 64 },
            codec: Codec::Lz4,
            volume: VolumeId(0),
            index: ColumnIndex::Plain,
            least_rows: None,
            least_value: None,
        }],
    };
    let store = ReelStore::open(io.clone(), &config).expect("open");
    let written: Vec<u8> = b"a value that compresses well. ".iter().copied().cycle().take(3_000).collect();
    store.put(COLUMN, b"packed", &written).expect("put");
    seal(&store, &io);
    let volume = &store.state.volumes[0];
    let read_at = store.state.key_read(COLUMN, b"packed", store.state.read_point(volume), false).expect("read");
    let row = in_place(store.state.search_sealed(&InPlace, volume, &read_at)).expect("search").expect("a sealed row");
    assert!((row.row.length.0 as usize) < written.len(), "the value was stored uncompressed");
    io.rot(&data_path(&volume.roots[0], row.segment), FileOffset(row.row.offset.0 + SMALL_HEAD_BYTES as u64 + 3), BitMask(1)).expect("rot");
    let mut buf = Vec::new();
    let found = store.get(COLUMN, b"packed", &mut buf);
    assert!(matches!(found, Err(StoreError::Corruption { .. })), "a flipped compressed payload answered: {found:?}");
    store.close().expect("close");
}

const PACED_KEYS: u64 = 20_000;
const PACED_FLOOR: u64 = 256 * 1024;
const BURST_FLOOR: u64 = 64 * 1024;
const BIG_BLOCK_KEYS: u64 = 2_000;

// a store with no callers lets the scrub climb past its floor, so a pass ends in well under the time the floor alone takes
#[test]
fn an_idle_store_scrubs_past_its_floor() {
    let io = SimulatedIo::new(Seed(13));
    let store = open(&io, 0);
    for number in 0..PACED_KEYS {
        store.put(COLUMN, &key(number), &vec![(number % 251) as u8; 400]).expect("put");
    }
    seal(&store, &io);
    let mut job = ScrubJob::new(Arc::clone(&store.state), 0);
    job.rate = PACED_FLOOR;
    let started = io.now();
    scrub_one_pass(&store, &io, &mut job);
    let took = io.now().0 - started.0;
    let at_floor = store.state.scrub_bytes.load(Ordering::Relaxed) * 1_000_000_000 / PACED_FLOOR;
    assert!(job.current_rate() > PACED_FLOOR, "the pace never left its floor");
    assert!(took < at_floor / 2, "the pass took {took} ns, and the floor alone takes {at_floor} ns");
    store.close().expect("close");
}

// a burst of caller gets drops a climbing scrub to its floor within the step that sees it
#[test]
fn a_burst_of_callers_drops_the_scrub_to_its_floor() {
    let io = SimulatedIo::new(Seed(14));
    let store = open(&io, 0);
    for number in 0..PACED_KEYS {
        store.put(COLUMN, &key(number), &vec![(number % 251) as u8; 400]).expect("put");
    }
    seal(&store, &io);
    let mut job = ScrubJob::new(Arc::clone(&store.state), 0);
    // a low floor keeps the pass running through the burst, since a scrub that ended its pass rests until the next
    job.rate = BURST_FLOOR;
    let mut buf = Vec::new();
    let mut step = |job: &mut ScrubJob<SimulatedIo>, gets: u64| {
        for number in 0..gets {
            buf.clear();
            store.get(COLUMN, &key(number % PACED_KEYS), &mut buf).expect("get");
        }
        io.advance_clock(Nanoseconds(250_000_000));
        let _step = job.step_unit(io.now());
    };
    for _quarter in 0..60 {
        step(&mut job, 2);
    }
    assert!(job.current_rate() > BURST_FLOOR, "the pace never left its floor");
    for _quarter in 0..4 {
        step(&mut job, 400);
    }
    assert!(store.state.scrub_passes.load(Ordering::Relaxed) == 0, "the pass ended before the burst");
    assert_eq!(job.current_rate(), BURST_FLOOR, "the burst left the scrub above its floor");
    store.close().expect("close");
}

// a step checks at most STEP_BYTES of records, and stops inside a row block that holds more, so the device never sees a long
// burst whatever the rate; the steps that follow carry on from where it stopped and the pass still checks every record
#[test]
fn a_step_stops_inside_its_block() {
    let io = SimulatedIo::new(Seed(15));
    let store = open(&io, 0);
    // a row block of 4 KiB values holds several MiB of records, far past one step
    for number in 0..BIG_BLOCK_KEYS {
        store.put(COLUMN, &key(number), &vec![(number % 251) as u8; 4096]).expect("put");
    }
    seal(&store, &io);
    let mut job = ScrubJob::new(Arc::clone(&store.state), 0);
    job.rate = 1024 * 1024 * 1024;
    let mut most = 0;
    let mut before = 0;
    while store.state.scrub_passes.load(Ordering::Relaxed) == 0 {
        match job.step_unit(io.now()) {
            JobStep::IdleUntil { deadline } => io.advance_clock(Nanoseconds(deadline.0.saturating_sub(io.now().0).max(1))),
            _other => {},
        }
        let now_checked = store.state.scrub_bytes.load(Ordering::Relaxed);
        most = most.max(now_checked - before);
        before = now_checked;
    }
    assert!(most <= STEP_BYTES + 8192, "one step checked {most} B");
    assert!(before >= BIG_BLOCK_KEYS * 4096, "the pass checked {before} B of {} B", BIG_BLOCK_KEYS * 4096);
    store.close().expect("close");
}

// the pace times its own direct probe, so a store whose blocks all sit in the page cache, as they do after an open, still gets a
// baseline and climbs, as does one on a file system that refuses reads that won't wait
#[test]
fn a_warm_store_still_paces() {
    for (warm_bytes, refuses) in [(usize::MAX, false), (0, false), (0, true)] {
        let io = SimulatedIo::new(Seed(16));
        let store = open(&io, 0);
        for number in 0..PACED_KEYS {
            store.put(COLUMN, &key(number), &vec![(number % 251) as u8; 400]).expect("put");
        }
        seal(&store, &io);
        io.set_warm_bytes(warm_bytes);
        io.set_refuses_no_wait(refuses);
        let mut job = ScrubJob::new(Arc::clone(&store.state), 0);
        job.rate = PACED_FLOOR;
        scrub_one_pass(&store, &io, &mut job);
        assert!(job.baseline().is_some(), "warm {warm_bytes} refused {refuses}: no baseline");
        assert!(job.current_rate() > PACED_FLOOR, "warm {warm_bytes} refused {refuses}: the pace never left its floor");
        io.set_warm_bytes(0);
        io.set_refuses_no_wait(false);
        store.close().expect("close");
    }
}

const LONG_KEYS: u64 = 40;
const LONG_VALUE: usize = 128 * 1024;

// a long record is read from the device through the segment's direct handle: a clean pass over long records finds nothing, and a
// byte flipped deep inside one is found and its segment marked
#[test]
fn long_records_are_checked_through_the_direct_handle() {
    let io = SimulatedIo::new(Seed(17));
    let store = open(&io, 0);
    for number in 0..LONG_KEYS {
        store.put(COLUMN, &key(number), &vec![(number % 251) as u8; LONG_VALUE]).expect("put");
    }
    let segment = seal(&store, &io);
    let mut job = ScrubJob::new(Arc::clone(&store.state), 0);
    job.rate = RATE;
    scrub_one_pass(&store, &io, &mut job);
    assert_eq!(store.state.scrub_failures.load(Ordering::Relaxed), 0, "a clean pass over long records failed one");
    assert!(store.state.scrub_bytes.load(Ordering::Relaxed) >= LONG_KEYS * LONG_VALUE as u64);

    let volume = &store.state.volumes[0];
    let name = key(LONG_KEYS / 2);
    let read_at = store.state.key_read(COLUMN, &name, store.state.read_point(volume), false).expect("read");
    let row = in_place(store.state.search_sealed(&InPlace, volume, &read_at)).expect("search").expect("a sealed row");
    io.rot(&data_path(&volume.roots[0], row.segment), FileOffset(row.row.offset.0 + row.row.length.0 - 5_000), BitMask(1)).expect("rot");
    io.advance_clock(SCRUB_PASS_REST);
    scrub_one_pass(&store, &io, &mut job);
    assert_eq!(store.state.scrub_failures.load(Ordering::Relaxed), 1, "the flipped byte in a long record went unseen");
    assert!(hold(&volume.damaged).contains(&segment));
    store.close().expect("close");
}

// a store whose direct reads are refused, as on a device whose blocks pass the alignment or a mount that takes the flag and not
// the read, checks every long record plainly, and the refusal marks nothing damaged
#[test]
fn refused_direct_reads_mark_nothing() {
    let io = SimulatedIo::new(Seed(18));
    let store = open(&io, 0);
    for number in 0..LONG_KEYS {
        store.put(COLUMN, &key(number), &vec![(number % 251) as u8; LONG_VALUE]).expect("put");
    }
    seal(&store, &io);
    io.set_direct_reads(DirectReads::Refused);
    let mut job = ScrubJob::new(Arc::clone(&store.state), 0);
    job.rate = RATE;
    scrub_one_pass(&store, &io, &mut job);
    assert_eq!(store.state.scrub_failures.load(Ordering::Relaxed), 0, "a refused direct read counted as rot");
    assert!(hold(&store.state.volumes[0].damaged).is_empty(), "a refused direct read marked damage");
    assert!(store.state.scrub_bytes.load(Ordering::Relaxed) >= LONG_KEYS * LONG_VALUE as u64, "the pass skipped records");
    io.set_direct_reads(DirectReads::Plain);
    store.close().expect("close");
}

// a disk copy that rotted under a clean copy in the page cache reads wrong through the direct handle, and the scrub marks it,
// though a plain read of the same record would pass
#[test]
fn rot_under_a_clean_cached_copy_is_found() {
    let io = SimulatedIo::new(Seed(19));
    let store = open(&io, 0);
    for number in 0..LONG_KEYS {
        store.put(COLUMN, &key(number), &vec![(number % 251) as u8; LONG_VALUE]).expect("put");
    }
    let segment = seal(&store, &io);
    io.set_direct_reads(DirectReads::Flipped);
    let mut job = ScrubJob::new(Arc::clone(&store.state), 0);
    job.rate = RATE;
    scrub_one_pass(&store, &io, &mut job);
    assert!(store.state.scrub_failures.load(Ordering::Relaxed) > 0, "rot read through the direct handle went unseen");
    assert!(hold(&store.state.volumes[0].damaged).contains(&segment));
    io.set_direct_reads(DirectReads::Plain);
    store.close().expect("close");
}

// a device error on a direct read is a bad sector under a good copy in the page cache: it stands as a failure and marks the
// segment, with no plain read the cache could answer, and the job goes on reading direct
#[test]
fn a_device_error_on_a_direct_read_is_found() {
    let io = SimulatedIo::new(Seed(20));
    let store = open(&io, 0);
    for number in 0..LONG_KEYS {
        store.put(COLUMN, &key(number), &vec![(number % 251) as u8; LONG_VALUE]).expect("put");
    }
    let segment = seal(&store, &io);
    io.set_direct_reads(DirectReads::Broken);
    let mut job = ScrubJob::new(Arc::clone(&store.state), 0);
    job.rate = RATE;
    scrub_one_pass(&store, &io, &mut job);
    assert!(store.state.scrub_failures.load(Ordering::Relaxed) > 0, "a device error on a direct read went unseen");
    assert!(hold(&store.state.volumes[0].damaged).contains(&segment));
    assert!(!job.is_direct_refused(), "a device error read as a refusal");
    io.set_direct_reads(DirectReads::Plain);
    store.close().expect("close");
}

// a volume's checks name it, count what its scrub checked and the passes it finished, and read a rate of 0 while it rests between
// passes, where the last pass's rate would read as a scrub still running
#[test]
fn a_volume_reports_its_scrub() {
    let io = SimulatedIo::new(Seed(21));
    let store = open(&io, 0);
    for number in 0..KEYS {
        store.put(COLUMN, &key(number), &value(number, 0)).expect("put");
    }
    seal(&store, &io);
    let mut job = ScrubJob::new(Arc::clone(&store.state), 0);
    job.rate = RATE;
    scrub_one_pass(&store, &io, &mut job);
    let checks: Vec<_> = store.volume_checks().collect();
    assert_eq!(checks.len(), 1);
    let check = checks[0];
    assert_eq!(check.name, "scrub");
    assert_eq!(check.scrub_passes, 1);
    assert_eq!(check.scrub_bytes, store.state.scrub_bytes.load(Ordering::Relaxed));
    assert!(check.scrub_bytes > 0);
    assert_eq!(check.scrub_bytes_per_second, 0, "a resting scrub read a rate");
    assert_eq!(check.damaged_segments, 0);
    store.close().expect("close");
}

// rot one sealed record past its head and let a scrub pass find it, and return the damaged segment and the rotten key
fn rot_and_scrub(store: &Store, io: &SimulatedIo) -> (SegmentNumber, Vec<u8>) {
    for number in 0..KEYS {
        store.put(COLUMN, &key(number), &value(number, 0)).expect("put");
    }
    let segment = seal(store, io);
    let volume = &store.state.volumes[0];
    let name = key(KEYS / 2);
    let read_at = store.state.key_read(COLUMN, &name, store.state.read_point(volume), false).expect("read");
    let row = in_place(store.state.search_sealed(&InPlace, volume, &read_at)).expect("search").expect("a sealed row");
    io.rot(&data_path(&volume.roots[0], row.segment), FileOffset(row.row.offset.0 + SMALL_HEAD_BYTES as u64), BitMask(1)).expect("rot");
    let mut job = ScrubJob::new(Arc::clone(&store.state), 0);
    job.rate = RATE;
    scrub_one_pass(store, io, &mut job);
    assert_eq!(store.damaged_segment_list(), vec![(VolumeId(0), segment)]);
    (segment, name)
}

// step a compact job of its own until `done` says so or it has nothing left to do, with the clock moving while it waits
fn compact_until(store: &Store, io: &SimulatedIo, done: impl Fn(&Store) -> bool) {
    let mut job = CompactJob::new(Arc::clone(&store.state), 0);
    let mut idle = 0;
    for _step in 0..STEPS {
        if done(store) || idle > BUSY_ROUNDS {
            return;
        }
        match job.step_work(io.now()) {
            JobStep::Worked => idle = 0,
            JobStep::IdleUntil { deadline } => io.advance_clock(Nanoseconds(deadline.0.saturating_sub(io.now().0).max(1))),
            JobStep::Idle | JobStep::Finished => {
                idle += 1;
                io.advance_clock(TICK);
            },
        }
        while io.step_any().is_some() {}
    }
}

// a scrub's damage clears once the caller puts back the key that read wrong and asks for a rebuild: the damaged keys are the
// rotten one alone, the merge leaves its record behind, and the clear outlives a reopen
#[test]
fn a_rebuild_after_the_put_back_clears_the_damage() {
    let io = SimulatedIo::new(Seed(22));
    let store = open(&io, 0);
    let (segment, name) = rot_and_scrub(&store, &io);
    let mut keys = Vec::new();
    store.damaged_keys(VolumeId(0), segment, |column, key| keys.push((column, key.to_vec()))).expect("damaged keys");
    assert_eq!(keys, vec![(COLUMN, name.clone())], "the damaged keys were other than the rotten one");
    store.put(COLUMN, &name, &value(KEYS / 2, 1)).expect("put back");
    let mut after = Vec::new();
    store.damaged_keys(VolumeId(0), segment, |column, key| after.push((column, key.to_vec()))).expect("damaged keys");
    assert!(after.is_empty(), "a key put back still counted as damaged");
    seal(&store, &io);
    store.rebuild_damaged(VolumeId(0), segment).expect("ask");
    compact_until(&store, &io, |store| store.damaged_segments() == 0);
    assert_eq!(store.damaged_segments(), 0, "the rebuild left the damage");
    assert!(store.damaged_segment_list().is_empty());
    assert!(store.state.volumes[0].damage.is_empty(), "the rebuild left spans");
    assert!(!store.state.volumes[0].table.holds(segment), "the rebuild left the damaged segment in place");
    let expected = |number: u64| value(number, u64::from(number == KEYS / 2));
    for number in 0..KEYS {
        assert_eq!(read(&store, &key(number)), Some(expected(number)));
    }
    store.close().expect("close");
    let store = open(&io, 0);
    assert_eq!(store.damaged_segments(), 0, "the clear came back damaged after a reopen");
    for number in 0..KEYS {
        assert_eq!(read(&store, &key(number)), Some(expected(number)));
    }
    store.close().expect("close");
}

// a rebuild asked before the rotten key goes back fails over the record it can't check, the segment stays damaged, and the ask
// ends, so the planner tries no failing merge each tick
#[test]
fn a_rebuild_over_a_rotten_record_keeps_the_damage() {
    let io = SimulatedIo::new(Seed(23));
    let store = open(&io, 0);
    let (segment, _name) = rot_and_scrub(&store, &io);
    store.rebuild_damaged(VolumeId(0), segment).expect("ask");
    compact_until(&store, &io, |store| store.state.merge_check_failures.load(Ordering::Relaxed) > 0);
    assert!(store.state.merge_check_failures.load(Ordering::Relaxed) > 0, "the merge never met the rotten record");
    assert_eq!(store.state.volumes[0].rebuild_asked.load(Ordering::Relaxed), NO_REBUILD, "a rebuild that failed a record's check kept its ask");
    assert_eq!(store.damaged_segment_list(), vec![(VolumeId(0), segment)]);
    assert!(store.state.volumes[0].table.holds(segment));
    store.close().expect("close");
}

// damage with no rot behind it, as a read the device failed once leaves, names no key to put back, and a rebuild still clears
// it though the merge drops nothing
#[test]
fn a_rebuild_clears_damage_no_record_holds() {
    let io = SimulatedIo::new(Seed(24));
    let store = open(&io, 0);
    for number in 0..KEYS {
        store.put(COLUMN, &key(number), &value(number, 0)).expect("put");
    }
    let segment = seal(&store, &io);
    store.state.damage_unread(0, segment);
    let mut keys = Vec::new();
    store.damaged_keys(VolumeId(0), segment, |column, key| keys.push((column, key.to_vec()))).expect("damaged keys");
    assert!(keys.is_empty(), "a clean segment named keys to put back");
    store.rebuild_damaged(VolumeId(0), segment).expect("ask");
    compact_until(&store, &io, |store| store.damaged_segments() == 0);
    assert_eq!(store.damaged_segments(), 0, "the rebuild left damage that no record holds");
    assert!(store.state.volumes[0].damage.is_empty());
    for number in 0..KEYS {
        assert_eq!(read(&store, &key(number)), Some(value(number, 0)));
    }
    store.close().expect("close");
}

// an asked rebuild waits behind another forced pick and still runs once that one lets it: the ask stays until the rebuild
// installs, where a planner that took it once would lose it to the first forced pick after
#[test]
fn an_asked_rebuild_outlasts_forced_picks() {
    const CONTESTED_TICKS: u64 = 50;
    let io = SimulatedIo::new(Seed(25));
    let store = open(&io, 0);
    let (segment, name) = rot_and_scrub(&store, &io);
    store.put(COLUMN, &name, &value(KEYS / 2, 1)).expect("put back");
    for number in 0..KEYS {
        store.put(COLUMN, &other_key(number), &value(number, 0)).expect("put");
    }
    let other = seal(&store, &io);
    let volume = &store.state.volumes[0];
    store.rebuild_damaged(VolumeId(0), segment).expect("ask");
    let forced_before = volume.picks[PickOutcome::Forced.index()].load(Ordering::Relaxed);
    let mut job = CompactJob::new(Arc::clone(&store.state), 0);
    for tick in 0..STEPS {
        if store.damaged_segments() == 0 {
            break;
        }
        // another forced pick stands at every tick for a while, as the cover pool's or a stuck seal's would
        if tick < CONTESTED_TICKS {
            job.rewrite.forced = job.rewrite.forced.or(Some(other));
        } else if tick == CONTESTED_TICKS {
            assert_eq!(volume.rebuild_asked.load(Ordering::Relaxed), segment.0, "the ask was lost behind other forced picks");
        }
        match job.step_work(io.now()) {
            JobStep::Worked => {},
            JobStep::IdleUntil { deadline } => io.advance_clock(Nanoseconds(deadline.0.saturating_sub(io.now().0).max(1))),
            JobStep::Idle | JobStep::Finished => io.advance_clock(TICK),
        }
        while io.step_any().is_some() {}
    }
    assert!(volume.picks[PickOutcome::Forced.index()].load(Ordering::Relaxed) > forced_before + 1, "no other pick was forced");
    assert_eq!(store.damaged_segments(), 0, "the asked rebuild never ran after the other forced picks");
    assert_eq!(volume.rebuild_asked.load(Ordering::Relaxed), NO_REBUILD);
    for number in 0..KEYS {
        assert_eq!(read(&store, &key(number)), Some(value(number, u64::from(number == KEYS / 2))));
    }
    store.close().expect("close");
}

fn other_key(number: u64) -> Vec<u8> {
    format!("other-{number:05}").into_bytes()
}

// a process stopped at any point of an asked rebuild opens again with every damaged segment one the table holds, and every key
// reads its value or answers Corruption while damage stays marked. Clearing again ends with no damage
#[test]
fn a_crash_inside_a_rebuild_leaves_damage_it_can_clear() {
    crash_inside_a_rebuild(Seed(26), |number| number == KEYS / 2);
}

// the same with every key put back elsewhere, so the rebuild keeps no row
#[test]
fn a_crash_inside_a_wholly_dead_rebuild_leaves_damage_it_can_clear() {
    crash_inside_a_rebuild(Seed(28), |_number| true);
}

// stop a rebuild at each operation in turn, after the keys `put_back` picks go back, and check what opens after
fn crash_inside_a_rebuild(seed: Seed, put_back: impl Fn(u64) -> bool) {
    const MOST_CRASH_POINTS: u64 = 2_000;
    let expected = |number: u64| value(number, u64::from(put_back(number)));
    let mut finished = false;
    for crash_at in 0..MOST_CRASH_POINTS {
        let io = SimulatedIo::new(seed);
        let store = open(&io, 0);
        let (segment, _name) = rot_and_scrub(&store, &io);
        for number in (0..KEYS).filter(|number| put_back(*number)) {
            store.put(COLUMN, &key(number), &value(number, 1)).expect("put back");
        }
        seal(&store, &io);
        store.close().expect("close");
        let store = open(&io, 0);
        assert_eq!(store.damaged_segment_list(), vec![(VolumeId(0), segment)], "the damage didn't outlive a close");

        store.rebuild_damaged(VolumeId(0), segment).expect("ask");
        io.arm_crash(OperationCount(crash_at));
        compact_until(&store, &io, |store| store.damaged_segments() == 0 || io.is_crashed());
        let is_crashed = io.is_crashed();
        io.disarm_crash();
        io.kill();
        drop(store);

        let store = open(&io, 0);
        for (volume, number) in store.damaged_segment_list() {
            assert!(store.state.volumes[volume.0 as usize].table.holds(number), "crash at {crash_at}: segment {number:?} is damaged and gone");
        }
        for number in 0..KEYS {
            let mut buf = Vec::new();
            match store.get(COLUMN, &key(number), &mut buf) {
                Ok(true) => assert_eq!(buf, expected(number), "crash at {crash_at}"),
                Err(StoreError::Corruption { .. }) => assert!(store.damaged_segments() > 0, "crash at {crash_at}: corrupt with no damage marked"),
                other => panic!("crash at {crash_at}: {other:?}"),
            }
        }
        for (volume, number) in store.damaged_segment_list() {
            let mut keys = Vec::new();
            store.damaged_keys(volume, number, |column, key| keys.push((column, key.to_vec()))).expect("damaged keys");
            assert!(keys.is_empty(), "crash at {crash_at}: a key put back counts as damaged");
            store.rebuild_damaged(volume, number).expect("ask");
            compact_until(&store, &io, |store| store.damaged_segments() == 0);
        }
        assert_eq!(store.damaged_segments(), 0, "crash at {crash_at}: clearing again left damage");
        for number in 0..KEYS {
            assert_eq!(read(&store, &key(number)), Some(expected(number)), "crash at {crash_at}");
        }
        store.close().expect("close");
        if !is_crashed {
            finished = true;
            break;
        }
    }
    assert!(finished, "the rebuild ran past {MOST_CRASH_POINTS} operations");
}

// a damaged segment whose every key went back elsewhere holds nothing live, so the rebuild keeps no row: it still clears the
// damage and ends the ask
#[test]
fn a_rebuild_of_a_wholly_dead_damaged_segment_clears_it() {
    let io = SimulatedIo::new(Seed(27));
    let store = open(&io, 0);
    let (segment, _name) = rot_and_scrub(&store, &io);
    for number in 0..KEYS {
        store.put(COLUMN, &key(number), &value(number, 1)).expect("put back");
    }
    seal(&store, &io);
    store.rebuild_damaged(VolumeId(0), segment).expect("ask");
    compact_until(&store, &io, |store| store.damaged_segments() == 0);
    assert_eq!(store.damaged_segments(), 0, "a wholly dead damaged segment stayed damaged");
    assert!(store.damaged_segment_list().is_empty());
    assert!(!store.state.volumes[0].table.holds(segment));
    assert_eq!(store.state.volumes[0].rebuild_asked.load(Ordering::Relaxed), NO_REBUILD);
    for number in 0..KEYS {
        assert_eq!(read(&store, &key(number)), Some(value(number, 1)));
    }
    store.close().expect("close");
    let store = open(&io, 0);
    assert_eq!(store.damaged_segments(), 0, "the clear came back damaged after a reopen");
    store.close().expect("close");
}

// put `keys` keys of `length` bytes and seal them, rot the middle byte of the middle key's record, a chunk's payload in a long one,
// then overwrite the odd keys and seal again, so a merge of the first segment drops rows. Merge it alone, and return it
fn merge_over_rot(io: &SimulatedIo, store: &Store, keys: u64, length: usize) -> SegmentNumber {
    for number in 0..keys {
        store.put(COLUMN, &key(number), &vec![(number % 251) as u8; length]).expect("put");
    }
    let first = seal(store, io);
    let volume = &store.state.volumes[0];
    let name = key(keys / 2);
    let read_at = store.state.key_read(COLUMN, &name, store.state.read_point(volume), false).expect("read");
    let row = in_place(store.state.search_sealed(&InPlace, volume, &read_at)).expect("search").expect("a sealed row");
    io.rot(&data_path(&volume.roots[0], row.segment), FileOffset(row.row.offset.0 + row.row.length.0 / 2), BitMask(1)).expect("rot");
    for number in (1..keys).step_by(2) {
        store.put(COLUMN, &key(number), &value(number, 1)).expect("overwrite");
    }
    seal(store, io);
    let mut merge = CompactJob::new(Arc::clone(&store.state), 0);
    merge.rewrite.forced = Some(first);
    // the store's own pick may have merged it already, in the same order
    if merge.rewrite.start() {
        while merge.rewrite.is_running() {
            merge.rewrite.step();
        }
    }
    settle(io);
    first
}

// an offset-order merge copies records as runs of bytes, and checks a long one in its copying pass: one rotten long record stops
// the merge, counts a check failure and marks its segment
#[test]
fn an_offset_order_merge_stops_at_a_rotten_long_record() {
    let io = SimulatedIo::new(Seed(29));
    let store = open(&io, 0);
    let first = merge_over_rot(&io, &store, LONG_KEYS, LONG_VALUE);
    let volume = &store.state.volumes[0];
    assert_eq!(volume.keyed_copies.load(Ordering::Relaxed), 0, "the merge went in key order");
    assert!(store.state.merge_check_failures.load(Ordering::Relaxed) > 0, "the merge carried the rotten record");
    assert!(volume.table.holds(first), "the merge installed over the rotten record");
    assert!(hold(&volume.damaged).contains(&first));
    store.close().expect("close");
}

// a short rotten record goes across unchecked, as before, and a trusted read's sum still finds it in its new segment
#[test]
fn an_offset_order_merge_carries_a_rotten_short_record_a_read_still_finds() {
    let io = SimulatedIo::new(Seed(30));
    let store = open(&io, 0);
    let first = merge_over_rot(&io, &store, KEYS, 100);
    let volume = &store.state.volumes[0];
    assert_eq!(volume.keyed_copies.load(Ordering::Relaxed), 0, "the merge went in key order");
    assert_eq!(store.state.merge_check_failures.load(Ordering::Relaxed), 0);
    assert!(!volume.table.holds(first), "the merge didn't install");
    assert!(matches!(store.get(COLUMN, &key(KEYS / 2), &mut Vec::new()), Err(StoreError::Corruption { .. })), "a read passed the rotten record");
    store.close().expect("close");
}

// a live walk's batch takes its point, then a segment written and sealed after that point lands before the batch seats its
// sources. The batches that follow, at points above its rows, see its delete, its overwrite and its new key
#[test]
fn a_live_walk_seats_a_segment_sealed_inside_its_first_batch() {
    let io = SimulatedIo::new(Seed(72));
    let store = open(&io, 0);
    let volume = &store.state.volumes[0];
    volume.holds.compaction.store(true, Ordering::Release);
    volume.holds.reclaim.store(true, Ordering::Release);
    for number in 0..KEYS {
        store.put(COLUMN, &key(number), &value(number, 0)).expect("put");
    }
    seal(&store, &io);
    let mut scan = store.iter_from(COLUMN, b"", Direction::Ascending).expect("scan");
    let (state, hook_io) = (Arc::clone(&store.state), io.clone());
    let ran = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let hook_ran = Arc::clone(&ran);
    volume.holds.set_between_point_and_seat(Some(reel::engine::volume::RunHook(Arc::new(move || {
        // a row under the batch's first keys, so the sealed segment's span starts where the walk seats its first sources
        state.write_value(COLUMN, &key(1), &value(1, 1), None).expect("put");
        state.write_grave(COLUMN, &key(150)).expect("delete");
        state.write_value(COLUMN, &key(160), &value(160, 1), None).expect("put");
        state.write_value(COLUMN, b"key-00170-new", b"new", None).expect("put");
        for volume in &state.volumes {
            state.seal_fullest(volume);
        }
        settle(&hook_io);
        hook_ran.store(true, Ordering::Release);
    }))));
    let (mut name, mut found) = (Vec::new(), Vec::new());
    let mut walked = std::collections::BTreeMap::new();
    while scan.next_row(&mut name, &mut found).expect("step") == ScanStep::Found {
        walked.insert(name.clone(), found.clone());
    }
    drop(scan);
    assert!(ran.load(Ordering::Acquire), "the hook never ran");
    assert!(hold(&volume.claims).open.iter().all(|segment| segment.row_count.0 == 0), "the hook's rows never sealed");
    // the store itself answers right: a get, and a walk started now
    assert_eq!(read(&store, &key(150)), None);
    assert_eq!(read(&store, &key(160)), Some(value(160, 1)));
    let mut fresh = std::collections::BTreeMap::new();
    let mut scan = store.iter_from(COLUMN, b"", Direction::Ascending).expect("scan");
    while scan.next_row(&mut name, &mut found).expect("step") == ScanStep::Found {
        fresh.insert(name.clone(), found.clone());
    }
    drop(scan);
    assert!(!fresh.contains_key(&key(150)) && fresh.get(&key(160)) == Some(&value(160, 1)) && fresh.contains_key(b"key-00170-new".as_slice()), "a fresh walk read wrong");
    // keys 150 and up come in batches whose points are above the sealed rows: the first batch holds 32 keys
    println!("walk: key 150 {:?}, key 160 {:?}, new key {:?}", walked.get(&key(150)).map(|v| String::from_utf8_lossy(v).to_string()), walked.get(&key(160)).map(|v| String::from_utf8_lossy(v).to_string()), walked.contains_key(b"key-00170-new".as_slice()));
    assert!(!walked.contains_key(&key(150)), "the walk answered a key deleted before its batch's point");
    assert_eq!(walked.get(&key(160)), Some(&value(160, 1)), "the walk answered a value overwritten before its batch's point");
    assert!(walked.contains_key(b"key-00170-new".as_slice()), "the walk missed a key written before its batch's point");
    store.close().expect("close");
}
