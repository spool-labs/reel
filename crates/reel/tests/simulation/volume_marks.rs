//! A landed mark a volume: a write waits for its own volume alone, and a cue still holds one moment across them

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

use reel::config::{ColumnConfig, ColumnIndex, KeyShape, StoreConfig, SyncPolicy, VolumeConfig};
use reel::engine::cue::Cue;
use reel::engine::reel_store::ReelStore;
use reel::engine::walk_job::FREE_IN_PLACE_BYTES;
use reel::engine::write_path::ClaimRequest;
use reel::guard::hold;
use reel::record::codec::Codec;
use reel::segment::segment_tail::SegmentNumber;
use reel_core::column_id::ColumnId;
use reel_core::units::{ByteCount, Nanoseconds, RowCount, VolumeId, WritePlace};
use reel_sim::seeded_random::Seed;
use reel_sim::simulated_io::SimulatedIo;
use reel_sim::wait::wait_until;

const BULK_ROOT: &str = "/disk/bulk";
const META_ROOT: &str = "/disk/meta";
const BULK: ColumnId = ColumnId(1);
const META: ColumnId = ColumnId(2);
const BUDGET: ByteCount = ByteCount(128 * 1024 * 1024);
const TICK: Nanoseconds = Nanoseconds(1_000_000);
const STEPS: u64 = 200_000;
const ONE_ROW: ClaimRequest = ClaimRequest {
    bytes: ByteCount(0),
    rows: RowCount(1),
    is_alone: false,
    has_records: false,
    covers: 0,
    mark: None,
    has_deletes: false,
};

type Store2 = ReelStore<SimulatedIo>;

fn volume(name: &str, root: &str) -> VolumeConfig {
    VolumeConfig {
        name: name.to_string(),
        roots: vec![PathBuf::from(root)],
        sync: SyncPolicy::Never,
        device: Default::default(), open_segments: None, segment_bytes: None, lifetime_groups: None,
    }
}

fn column(id: ColumnId, name: &str, volume: u8) -> ColumnConfig {
    ColumnConfig {
        id,
        name: name.to_string(),
        key_shape: KeyShape::Variable { most: 64 },
        codec: Codec::None,
        volume: VolumeId(volume),
        index: ColumnIndex::Plain,
        least_rows: None,
        least_value: None,
    }
}

fn open(io: &SimulatedIo) -> Store2 {
    let config = StoreConfig {
        memory_budget: BUDGET,
        threads: reel::config::NO_WORKERS,
        volumes: vec![volume("bulk", BULK_ROOT), volume("meta", META_ROOT)],
        columns: vec![column(BULK, "bulk", 0), column(META, "meta", 1)],
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

fn read(store: &Store2, column: ColumnId, key: &[u8]) -> Option<Vec<u8>> {
    let mut buf = Vec::new();
    store.get(column, key, &mut buf).expect("get").then_some(buf)
}

// a put on one volume returns and shows while a write on another is still in flight
#[test]
fn put_passes_a_write_in_flight() {
    let io = SimulatedIo::new(Seed(1));
    let store = open(&io);
    let bulk = &store.state.volumes[0];
    let claim = store.state.claim(bulk, &ONE_ROW).expect("claim");

    store.put(META, b"key", b"value").expect("put");
    assert_eq!(read(&store, META, b"key"), Some(b"value".to_vec()));

    drop(claim);
    store.close().expect("close");
}

// a write held up on a volume keeps its later writes from its own reads, and a cue keeps the same cut
#[test]
fn cue_holds_each_volume_at_its_mark() {
    let io = SimulatedIo::new(Seed(2));
    let store = open(&io);
    let bulk = &store.state.volumes[0];
    store.put(BULK, b"before", b"value").expect("put");
    let bulk_mark = bulk.order.mark();
    let claim = store.state.claim(bulk, &ONE_ROW).expect("claim");
    store.put(META, b"key", b"value").expect("put");

    let cue = store.cue().expect("cue");
    assert_eq!(cue.point_of(BULK).expect("point"), bulk_mark);
    assert!(cue.point_of(META).expect("point") > claim.first_place());
    let mut buf = Vec::new();
    assert!(cue.get(META, b"key", &mut buf).expect("get"));
    assert!(cue.get(BULK, b"before", &mut buf).expect("get"));

    drop(claim);
    store.put(META, b"later", b"value").expect("put");
    assert!(!cue.get(META, b"later", &mut buf).expect("get"));
    drop(cue);
    store.close().expect("close");
}

// a write that returned before another began is in every cue that holds the later one
#[test]
fn cut_keeps_write_order() {
    let io = SimulatedIo::new(Seed(3));
    let store = open(&io);
    let mut buf = Vec::new();
    for number in 0..200_u64 {
        let value = number.to_le_bytes();
        store.put(META, b"first", &value).expect("put");
        let between = store.cue().expect("cue");
        store.put(BULK, b"second", &value).expect("put");
        let after = store.cue().expect("cue");

        assert!(between.get(META, b"first", &mut buf).expect("get"));
        assert_eq!(buf, value);
        if number > 0 {
            assert!(between.get(BULK, b"second", &mut buf).expect("get"));
            assert_eq!(buf, (number - 1).to_le_bytes());
        }
        assert!(after.get(BULK, b"second", &mut buf).expect("get"));
        assert_eq!(buf, value);
        assert!(after.get(META, b"first", &mut buf).expect("get"));
        assert_eq!(buf, value);
    }
    store.close().expect("close");
}

// a cue cut at an earlier count holds neither write that landed past it, and above all not the later one without the earlier
#[test]
fn cut_caps_every_mark() {
    let io = SimulatedIo::new(Seed(8));
    let store = open(&io);
    let counts = store.state.cues.counts();
    let cut = store.state.places.last_given();
    store.put(META, b"first", b"value").expect("put");
    store.put(BULK, b"second", b"value").expect("put");

    let cue = Cue::pin_at(&store.state, counts, cut).expect("a slot").expect("no job crossed it");
    let mut buf = Vec::new();
    assert!(!cue.get(META, b"first", &mut buf).expect("get"));
    assert!(!cue.get(BULK, b"second", &mut buf).expect("get"));
    drop(cue);
    store.close().expect("close");
}

// a cue over a volume nothing has written reads nothing there and everything elsewhere
#[test]
fn cue_on_a_volume_never_written() {
    let io = SimulatedIo::new(Seed(4));
    let store = open(&io);
    store.put(META, b"key", b"value").expect("put");
    let cue = store.cue().expect("cue");
    let mut buf = Vec::new();
    assert!(cue.get(META, b"key", &mut buf).expect("get"));
    assert!(!cue.get(BULK, b"key", &mut buf).expect("get"));
    drop(cue);
    store.close().expect("close");
}

// a crash with rows on one volume restarts the count and every mark above them, the volume with nothing included
#[test]
fn open_lifts_every_mark() {
    let io = SimulatedIo::new(Seed(5));
    let store = open(&io);
    for number in 0..50_u64 {
        store.put(META, &number.to_le_bytes(), b"value").expect("put");
    }
    store.flush().expect("flush");
    settle(&io);
    let highest = store.state.read_point(&store.state.volumes[1]);
    io.power_cut();
    drop(store);

    let store = open(&io);
    assert!(store.state.places.last_given() >= highest);
    for volume in &store.state.volumes {
        assert!(volume.order.mark() >= highest);
    }
    let cue = store.cue().expect("cue");
    let mut buf = Vec::new();
    assert!(cue.get(META, &0_u64.to_le_bytes(), &mut buf).expect("get"));
    drop(cue);
    store.put(BULK, b"key", b"value").expect("put");
    assert!(store.state.read_point(&store.state.volumes[0]) > highest);
    store.close().expect("close");
}

// a seal on one volume starts and lands while a write on another stands in flight
#[test]
fn seal_passes_a_write_in_flight() {
    let io = SimulatedIo::new(Seed(6));
    let store = open(&io);
    store.put(META, b"key", b"value").expect("put");
    let claim = store.state.claim(&store.state.volumes[0], &ONE_ROW).expect("claim");
    store.state.seal_fullest(&store.state.volumes[1]);
    settle(&io);
    let seals = store.state.volumes[1].seals_done.load(Ordering::Acquire);
    assert_eq!(seals, 1);
    assert_eq!(read(&store, META, b"key"), Some(b"value".to_vec()));
    drop(claim);
    store.close().expect("close");
}

// a place stands once across the volumes, so nothing two volumes write shares one
#[test]
fn places_stay_store_wide() {
    let io = SimulatedIo::new(Seed(7));
    let store = open(&io);
    store.put(BULK, b"key", b"value").expect("put");
    let bulk = store.state.volumes[0].order.mark();
    store.put(META, b"key", b"value").expect("put");
    let meta = store.state.volumes[1].order.mark();
    assert!(meta > bulk);
    store.close().expect("close");
}

fn seal_meta(store: &Store2, io: &SimulatedIo) -> SegmentNumber {
    let number = hold(&store.state.volumes[1].claims).open[0].number;
    store.state.seal_fullest(&store.state.volumes[1]);
    settle(io);
    number
}

// a cue cut around a put held up on meta: its meta point stands below that put and its bulk point past it
fn cue_around_put(store: &Store2, key: &[u8], value: &[u8]) -> Cue<SimulatedIo> {
    let meta = &store.state.volumes[1];
    let earlier = store.state.claim(meta, &ONE_ROW).expect("claim");
    thread::scope(|scope| {
        let writer = scope.spawn(|| store.put(META, key, value));
        let place = loop {
            if let Some(row) = store.state.index.lookup(META, key, WritePlace(u64::MAX), meta.view()).row {
                break row.place;
            }
            thread::yield_now();
        };
        store.put(BULK, b"past", b"value").expect("put");
        let cue = store.cue().expect("cue");
        assert!(cue.point_of(META).expect("point") < place);
        assert!(cue.point_of(BULK).expect("point") > place);
        drop(earlier);
        writer.join().expect("join").expect("put");
        cue
    })
}

// a cue cut after a bulk write that stands between a meta key's two rows, so its bulk point sits between them and its meta
// point sits past them
fn cue_past_rows(store: &Store2, keys: &[&[u8]], value: &[u8]) -> Cue<SimulatedIo> {
    store.put(BULK, b"between", b"value").expect("put");
    for key in keys {
        store.put(META, key, value).expect("put");
    }
    let cue = store.cue().expect("cue");
    assert!(cue.point_of(BULK).expect("point") < cue.point_of(META).expect("point"));
    cue
}

// meta's rows of one sealed segment, with a live row, and the key a cue will hold written last
fn write_older(store: &Store2, io: &SimulatedIo) -> SegmentNumber {
    for key in [&b"keep"[..], b"b", b"c", b"d", b"a"] {
        store.put(META, key, b"first").expect("put");
    }
    seal_meta(store, io)
}

fn release_compaction(store: &Store2, io: &SimulatedIo) {
    store.state.volumes[1].holds.compaction.store(false, Ordering::Release);
    store.state.ask_compaction();
    settle(io);
}

// a cue's low point on the second volume keeps a dead segment there, with its point on the first past every replacement
#[test]
fn second_point_holds_an_unlink() {
    let io = SimulatedIo::new(Seed(9));
    let store = open(&io);
    store.put(META, b"key", b"older").expect("put");
    let older = seal_meta(&store, &io);
    let cue = cue_around_put(&store, b"key", b"newer");
    seal_meta(&store, &io);

    let meta = &store.state.volumes[1];
    assert!(meta.table.holds(older));
    let mut buf = Vec::new();
    assert!(cue.get(META, b"key", &mut buf).expect("get"));
    assert_eq!(buf, b"older");

    drop(cue);
    settle(&io);
    assert!(!meta.table.holds(older));
    store.close().expect("close");
}

// a cue's low point on the first volume holds no dead segment back on the second
#[test]
fn first_point_holds_no_unlink() {
    let io = SimulatedIo::new(Seed(10));
    let store = open(&io);
    store.put(META, b"key", b"older").expect("put");
    let older = seal_meta(&store, &io);
    let cue = cue_past_rows(&store, &[b"key"], b"newer");
    seal_meta(&store, &io);

    assert!(!store.state.volumes[1].table.holds(older));
    let mut buf = Vec::new();
    assert!(cue.get(META, b"key", &mut buf).expect("get"));
    assert_eq!(buf, b"newer");
    drop(cue);
    store.close().expect("close");
}

// a rewrite on the second volume keeps a replaced row a cue reads there, with the cue's point on the first past the replacement
#[test]
fn second_point_keeps_a_row() {
    let io = SimulatedIo::new(Seed(11));
    let store = open(&io);
    let meta = &store.state.volumes[1];
    meta.holds.compaction.store(true, Ordering::Release);
    let older = write_older(&store, &io);
    for key in [&b"b"[..], b"c", b"d"] {
        store.put(META, key, b"second").expect("put");
    }
    let cue = cue_around_put(&store, b"a", b"second");
    seal_meta(&store, &io);
    release_compaction(&store, &io);

    assert!(!meta.table.holds(older));
    assert_eq!(read(&store, META, b"a"), Some(b"second".to_vec()));
    assert_eq!(read(&store, META, b"keep"), Some(b"first".to_vec()));
    let mut buf = Vec::new();
    assert!(cue.get(META, b"a", &mut buf).expect("get"));
    assert_eq!(buf, b"first");
    drop(cue);
    store.close().expect("close");
}

// a rewrite on the second volume drops a replaced row with only the cue's point on the first between its two rows
#[test]
fn first_point_keeps_no_row() {
    let io = SimulatedIo::new(Seed(12));
    let store = open(&io);
    let meta = &store.state.volumes[1];
    meta.holds.compaction.store(true, Ordering::Release);
    let older = write_older(&store, &io);
    let cue = cue_past_rows(&store, &[b"a", b"b", b"c", b"d"], b"second");
    seal_meta(&store, &io);
    release_compaction(&store, &io);

    assert!(!meta.table.holds(older));
    let output = hold(&meta.summary).rows().map(|row| row.facts.segment).max().expect("a row");
    let rows = hold(&meta.summary).get(output).expect("a row").cursor().column().expect("a column").dead_rows.0;
    assert_eq!(rows, 0);
    drop(cue);
    store.close().expect("close");
}

fn large(byte: u8) -> Vec<u8> {
    vec![byte; FREE_IN_PLACE_BYTES.0 as usize]
}

// keep a segment's dead share under what triggers a rewrite
fn keep_live(store: &Store2) {
    store.put(META, b"keep", b"value").expect("put");
    store.put(META, b"keep-1", &large(3)).expect("put");
    store.put(META, b"keep-2", &large(4)).expect("put");
    store.put(META, b"keep-3", &large(5)).expect("put");
    store.put(META, b"keep-4", &large(6)).expect("put");
}

// a free on the second volume waits for a cue's low point there, with its point on the first past the replacement
#[test]
fn second_point_keeps_blocks() {
    let io = SimulatedIo::new(Seed(13));
    let store = open(&io);
    keep_live(&store);
    store.put(META, b"key", &large(1)).expect("put");
    seal_meta(&store, &io);
    let cue = cue_around_put(&store, b"key", &large(2));
    seal_meta(&store, &io);

    let meta = &store.state.volumes[1];
    assert_eq!(meta.bytes_freed.load(Ordering::Acquire), 0);
    let mut buf = Vec::new();
    assert!(cue.get(META, b"key", &mut buf).expect("get"));
    assert_eq!(buf, large(1));

    drop(cue);
    settle(&io);
    assert!(meta.bytes_freed.load(Ordering::Acquire) > 0);
    store.close().expect("close");
}

// a free on the second volume runs with only the cue's point on the first between the record and its replacement
#[test]
fn first_point_keeps_no_blocks() {
    let io = SimulatedIo::new(Seed(14));
    let store = open(&io);
    keep_live(&store);
    store.put(META, b"key", &large(1)).expect("put");
    seal_meta(&store, &io);
    let cue = cue_past_rows(&store, &[b"key"], &large(2));
    seal_meta(&store, &io);

    assert!(store.state.volumes[1].bytes_freed.load(Ordering::Acquire) > 0);
    assert_eq!(read(&store, META, b"key"), Some(large(2)));
    drop(cue);
    store.close().expect("close");
}

// a flush finds its slot under its own volume's lock, so a writer on one volume never waits on another's
#[test]
fn flush_locks_its_own_volume() {
    let io = SimulatedIo::new(Seed(62));
    let store = open(&io);
    store.put(BULK, b"bulk-key", b"value").expect("put");
    store.put(META, b"meta-key", b"value").expect("put");
    let state = &store.state;
    let meta_slot = hold(&state.volumes[1].claims).open.first().map(|segment| segment.slot).expect("an open meta segment");

    let is_pinned = AtomicBool::new(false);
    let bulk_claims = hold(&state.volumes[0].claims);
    thread::scope(|scope| {
        scope.spawn(|| {
            let pinned = state.pin_rows(meta_slot);
            is_pinned.store(pinned.is_some(), Ordering::Release);
        });
        wait_until(|| is_pinned.load(Ordering::Acquire), "a meta flush waited on the bulk volume's lock");
    });
    drop(bulk_claims);
    store.close().expect("close");
}
