//! Filter blocks stay in memory while they fit, so a get that misses reads nothing

use std::path::{Path, PathBuf};

use reel::config::{ColumnConfig, KeyShape, StoreConfig, SyncPolicy, VolumeConfig};
use reel::engine::open_segment::data_path;
use reel::engine::reel_store::ReelStore;
use reel::io::Io;
use reel::record::codec::Codec;
use reel_core::column_id::ColumnId;
use reel_core::maintenance_report::MaintenanceReport;
use reel_core::units::{ByteCount, FileOffset, Nanoseconds, RowCount, SegmentCount, VolumeId};
use reel_sim::file_image::BitMask;
use reel_sim::seeded_random::Seed;
use reel_sim::simulated_io::SimulatedIo;

const ROOT: &str = "/disk/filters";
const COLUMN: ColumnId = ColumnId(1);
const COMPLETE: ColumnId = ColumnId(2);
const BUDGET: ByteCount = ByteCount(64 * 1024 * 1024);
const TICK: Nanoseconds = Nanoseconds(1_000_000);
const STEPS: u64 = 400_000;
const KEYS: u64 = 3_000;
const MISSES: u64 = 500;
// a filter answers yes for about one miss in 600, and each such miss reads its block
const MOST_FALSE_READS: u64 = 4;
// the most a point read reads for one row
const MOST_BLOCK_BYTES: u64 = 16 * 1024;

type Store = ReelStore<SimulatedIo>;

fn config() -> StoreConfig {
    StoreConfig {
        memory_budget: BUDGET,
        threads: reel::config::NO_WORKERS,
        volumes: vec![VolumeConfig {
            name: "filters".to_string(),
            roots: vec![PathBuf::from(ROOT)],
            sync: SyncPolicy::Never,
            device: Default::default(), open_segments: None,
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

fn open(io: &SimulatedIo) -> Store {
    ReelStore::open(io.clone(), &config()).expect("open")
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

fn key(number: u64) -> Vec<u8> {
    format!("key-{number:06}").into_bytes()
}

fn filled(io: &SimulatedIo) -> Store {
    let store = open(io);
    for number in 0..KEYS {
        store.put(COLUMN, &key(2 * number), b"value").expect("put");
    }
    store.seal_now();
    settle(io);
    store
}

// a sealed segment's filters come into memory with no read asked for, and misses then read only their false positives
#[test]
fn misses_read_nothing() {
    let io = SimulatedIo::new(Seed(1));
    let store = filled(&io);
    let before = io.read_count().0;
    let mut buf = Vec::new();
    for number in 0..MISSES {
        assert!(!store.get(COLUMN, &key(2 * number + 1), &mut buf).expect("get"));
    }
    assert!(io.read_count().0 - before <= MOST_FALSE_READS);
    store.close().expect("close");
}

// an open starts with nothing resident and the loader brings the filters back, with reads answering all the while
#[test]
fn reopen_loads_again() {
    let io = SimulatedIo::new(Seed(2));
    let store = filled(&io);
    store.close().expect("close");
    drop(store);

    let store = open(&io);
    let mut buf = Vec::new();
    assert!(store.get(COLUMN, &key(0), &mut buf).expect("get"));
    assert!(!store.get(COLUMN, &key(1), &mut buf).expect("get"));
    settle(&io);
    let before = io.read_count().0;
    assert!(!store.get(COLUMN, &key(3), &mut buf).expect("get"));
    assert_eq!(io.read_count().0, before);
    store.close().expect("close");
}

// the reports say the arena isn't full until the loader has run out, and give the fill's time from then on
#[test]
fn reports_show_the_fill() {
    let io = SimulatedIo::new(Seed(6));
    let store = filled(&io);
    store.close().expect("close");
    drop(store);

    let store = open(&io);
    let mut report = MaintenanceReport::new();
    store.maintenance_report(&mut report);
    assert!(!store.memory_report().is_arena_full);
    assert_eq!(report.arena_fill, None);

    settle(&io);
    store.maintenance_report(&mut report);
    assert!(store.memory_report().is_arena_full);
    assert!(report.arena_fill.is_some());
    assert_eq!(store.memory_report().arena_segments_left_out, SegmentCount(0));

    // an empty extent says the arena tried and had no room
    let segment = reel::guard::hold(&store.state.volumes[0].summary).rows().next().expect("a row").facts.segment;
    store.state.filter_arena.drop_segment(segment);
    assert!(store.state.filter_arena.begin(segment, 0, 0));
    let memory = store.memory_report();
    assert_eq!(memory.arena_segments_left_out, SegmentCount(1));
    assert_eq!(memory.arena_columns_left_out, 1);
    store.close().expect("close");
}

// a probe that finds its extent mid-change says nothing, and the get answers from the footer's own filter
#[test]
fn probe_mid_change_reads_the_footer() {
    let io = SimulatedIo::new(Seed(5));
    let store = filled(&io);
    let segment = reel::guard::hold(&store.state.volumes[0].summary).rows().next().expect("a row").facts.segment;
    let arena = &store.state.filter_arena;
    arena.drop_segment(segment);
    assert!(arena.begin(segment, 2, 8));

    let mut buf = Vec::new();
    let before = io.read_count().0;
    assert!(!store.get(COLUMN, &key(1), &mut buf).expect("get"));
    assert!(io.read_count().0 > before);
    assert!(store.get(COLUMN, &key(0), &mut buf).expect("get"));
    store.close().expect("close");
}

// a segment's going takes its filters with it
#[test]
fn unlink_frees_the_room() {
    let io = SimulatedIo::new(Seed(3));
    let store = filled(&io);
    let segment = reel::guard::hold(&store.state.volumes[0].summary).rows().next().expect("a row").facts.segment;
    assert!(store.state.filter_arena.holds(segment));
    for number in 0..KEYS {
        store.delete(COLUMN, &key(2 * number)).expect("delete");
    }
    store.seal_now();
    settle(&io);
    store.state.ask_compaction();
    settle(&io);
    assert!(!store.state.filter_arena.holds(segment));
    store.close().expect("close");
}

// a segment with no filters to hold takes no room, and the older segments behind it still load
#[test]
fn passes_a_segment_without_filters() {
    let io = SimulatedIo::new(Seed(4));
    let mut config = config();
    config.columns.push(ColumnConfig {
        id: COMPLETE,
        name: "complete".to_string(),
        key_shape: KeyShape::Variable { most: 64 },
        codec: Codec::None,
        volume: VolumeId(0),
        complete_rows: Some(RowCount(KEYS)),
        sorted_rows: None,
        least_value: None,
    });
    // the budget pays for the table, so this store takes a larger one
    config.memory_budget = ByteCount(128 * 1024 * 1024);
    let store = ReelStore::open(io.clone(), &config).expect("open");
    for number in 0..KEYS {
        store.put(COLUMN, &key(2 * number), b"value").expect("put");
    }
    store.seal_now();
    settle(&io);
    store.put(COMPLETE, b"only", b"value").expect("put");
    store.seal_now();
    settle(&io);
    store.close().expect("close");
    drop(store);

    let store = ReelStore::open(io.clone(), &config).expect("open");
    settle(&io);
    let before = io.read_count().0;
    let mut buf = Vec::new();
    assert!(!store.get(COLUMN, &key(1), &mut buf).expect("get"));
    assert_eq!(io.read_count().0, before);
    store.close().expect("close");
}

// a filter that fails its check keeps its segment out of memory, and the loader moves on and goes quiet
#[test]
fn rotted_filter_stays_out() {
    let io = SimulatedIo::new(Seed(5));
    let store = filled(&io);
    let segment = reel::guard::hold(&store.state.volumes[0].summary).rows().next().expect("a row").facts.segment;
    let (line, _replacement) = store.state.volumes[0].table.block_line(segment, 0).expect("a block");
    store.close().expect("close");
    drop(store);
    let listing = io.list_directory(Path::new(ROOT)).expect("list");
    let path = listing.into_iter().find(|path| path.extension().is_some_and(|name| name == "data")).expect("a segment");
    io.rot(&path, FileOffset(u64::from(line.filter_offset)), BitMask(1)).expect("rot");

    let store = open(&io);
    settle(&io);
    assert!(store.state.filter_arena.copy_filter(segment, line.filter_offset).is_none());
    store.close().expect("close");
}

// a rewrite's inputs take their filters with them
#[test]
fn rewrite_frees_the_room() {
    let io = SimulatedIo::new(Seed(6));
    let store = filled(&io);
    let segment = reel::guard::hold(&store.state.volumes[0].summary).rows().next().expect("a row").facts.segment;
    assert!(store.state.filter_arena.holds(segment));
    for number in 0..KEYS * 3 / 4 {
        store.put(COLUMN, &key(2 * number), b"newer").expect("put");
    }
    store.seal_now();
    settle(&io);
    cool(&store, &io);
    store.state.ask_compaction();
    settle(&io);
    let rows = reel::guard::hold(&store.state.volumes[0].summary);
    assert!(rows.rows().all(|row| row.facts.segment != segment));
    drop(rows);
    assert!(!store.state.filter_arena.holds(segment));
    store.close().expect("close");
}

// more segments with a rotted filter than any list could hold still leave the loader quiet, and each answers through the cache
#[test]
fn many_rotted_filters_go_quiet() {
    const SEGMENTS: u64 = 80;
    let io = SimulatedIo::new(Seed(7));
    let store = open(&io);
    let mut lines = Vec::new();
    for number in 0..SEGMENTS {
        store.put(COLUMN, &key(number), b"value").expect("put");
        store.seal_now();
        settle(&io);
    }
    let rows: Vec<_> = reel::guard::hold(&store.state.volumes[0].summary).rows().map(|row| row.facts.segment).collect();
    for segment in rows {
        let (line, _replacement) = store.state.volumes[0].table.block_line(segment, 0).expect("a block");
        lines.push((segment, line.filter_offset));
    }
    store.close().expect("close");
    drop(store);
    for (segment, offset) in lines {
        io.rot(data_path(Path::new(ROOT), segment).as_ref(), FileOffset(u64::from(offset)), BitMask(1)).expect("rot");
    }

    let store = open(&io);
    settle(&io);
    let mut buf = Vec::new();
    assert!(matches!(store.get(COLUMN, &key(0), &mut buf), Ok(true) | Err(_)));
    store.close().expect("close");
}

// the writer pauses once a seal lands, so the hot window closes and the rewrites hot ingest put off can start
fn cool(store: &Store, io: &SimulatedIo) {
    let until = store.state.quiet_at(io.clock_reading());
    io.advance_clock(Nanoseconds(until.0.saturating_sub(io.clock_reading().0)));
    settle(io);
}

// a get reads one row block for its key, so blocks stay small and a sealed segment of a few thousand rows has many
#[test]
fn row_blocks_stay_small() {
    let io = SimulatedIo::new(Seed(2));
    let store = filled(&io);
    let volume = &store.state.volumes[0];
    let segment = reel::guard::hold(&volume.summary).rows().map(|row| row.facts.segment).next().expect("a sealed segment");
    let (first, count) = volume.table.column_blocks(segment, COLUMN).expect("the column");
    assert!(count > 1);
    for index in first..first + count {
        let block = volume.table.walk_block(segment, index).expect("a block");
        assert!(block.entry.block_length().0 <= MOST_BLOCK_BYTES, "block {index}");
    }
    store.close().expect("close");
}
