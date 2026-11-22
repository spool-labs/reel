//! A landed mark a volume: a write waits for its own volume alone, and a cue still holds one moment across them

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

use reel::config::{ColumnConfig, ColumnIndex, KeyShape, StoreConfig, SyncPolicy, VolumeConfig};
use reel::engine::cue::Cue;
use reel::engine::reel_store::ReelStore;
use reel::engine::volume::NO_SEAL;
use reel::engine::walk_job::FREE_IN_PLACE_BYTES;
use std::io::IoSlice;

use reel::engine::write_path::{ClaimRequest, HEAD_PAGE_BYTES, LandOutcome};
use reel::index::write_row::WriteRow;
use reel::io::IoFile;
use reel::record::record_check::RecordIdentity;
use reel::record::record_encoder::PlainRecord;
use reel::record::record_head::plain_length;
use reel::row_kind::RowKind;
use reel::rows::row_group::row_size;
use reel::guard::hold;
use reel::record::codec::Codec;
use reel::segment::segment_tail::SegmentNumber;
use reel_core::column_id::ColumnId;
use reel_core::units::{ByteCount, Nanoseconds, RowCount, VolumeId, WritePlace};
use reel_sim::seeded_random::Seed;
use reel_sim::simulated_disk::OperationCount;
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

// a claim in flight when a seal picks its segment, which then ends without landing, leaves the volume counting the rows it holds:
// the seal takes back only its drained rows, so a row that lands in the next segment stays counted and every scan finds it
#[test]
fn a_claim_dropped_under_a_seal_leaves_the_count_whole() {
    let io = SimulatedIo::new(Seed(9));
    let config = StoreConfig {
        memory_budget: BUDGET,
        threads: reel::config::NO_WORKERS,
        volumes: vec![VolumeConfig { open_segments: Some(1), ..volume("bulk", BULK_ROOT) }, volume("meta", META_ROOT)],
        columns: vec![column(BULK, "bulk", 0), column(META, "meta", 1)],
    };
    let store = ReelStore::open(io.clone(), &config).expect("open");
    let bulk = &store.state.volumes[0];
    for number in 0..100_u64 {
        store.put(BULK, &number.to_be_bytes(), b"value").expect("put");
    }
    let claim = store.state.claim(bulk, &ONE_ROW).expect("claim");
    store.seal_now();
    for _step in 0..STEPS {
        if bulk.sealing.load(Ordering::Acquire) != NO_SEAL {
            break;
        }
        if io.step_any().is_none() {
            io.advance_clock(TICK);
        }
    }
    assert_ne!(bulk.sealing.load(Ordering::Acquire), NO_SEAL, "the seal never picked its segment");
    drop(claim);
    store.put(BULK, b"later", b"value").expect("put");
    settle(&io);
    assert_eq!(bulk.seals_done.load(Ordering::Acquire), 1);
    assert_eq!(bulk.rows_in_index.load(Ordering::Acquire), 1, "the volume counts the one open row");

    let mut scan = store.iter_from(BULK, b"later", reel_core::direction::Direction::Ascending).expect("scan");
    let mut name = Vec::new();
    assert_eq!(scan.next_key(&mut name).expect("step"), reel_core::scan::ScanStep::Found);
    assert_eq!(name, b"later");
    drop(scan);
    store.close().expect("close");
}

// a claim that takes its places late lands a row the pick's copy of the segment's places can't hold, so a seal that picked
// the segment while the claim was in flight reads the places again once its writers drain
fn seal_with_a_late_claim(earlier_rows: u64) {
    let io = SimulatedIo::new(Seed(11));
    let config = StoreConfig {
        memory_budget: BUDGET,
        threads: reel::config::NO_WORKERS,
        volumes: vec![VolumeConfig { open_segments: Some(1), ..volume("bulk", BULK_ROOT) }, volume("meta", META_ROOT)],
        columns: vec![column(BULK, "bulk", 0), column(META, "meta", 1)],
    };
    let store = ReelStore::open(io.clone(), &config).expect("open");
    let bulk = &store.state.volumes[0];
    for number in 0..earlier_rows {
        store.put(BULK, &number.to_be_bytes(), b"value").expect("put");
    }

    let (key, value) = (b"late".as_slice(), vec![5u8; 300]);
    let length = plain_length(ByteCount(value.len() as u64));
    let request = ClaimRequest { bytes: length, has_records: true, ..ONE_ROW };
    let (mut claim, room) = store.state.claim_room_late(bulk, &request, row_size(key.len()), true).expect("claim");
    store.seal_now();
    for _step in 0..STEPS {
        if bulk.sealing.load(Ordering::Acquire) != NO_SEAL {
            break;
        }
        if io.step_any().is_none() {
            io.advance_clock(TICK);
        }
    }
    assert_ne!(bulk.sealing.load(Ordering::Acquire), NO_SEAL, "the seal never picked its segment");

    store.state.take_late_ticket(&mut claim).expect("ticket");
    let place = claim.first_place();
    let identity = RecordIdentity { record_key: store.state.record_key, column: BULK, key, kind: RowKind::Value, place: WritePlace(0) };
    let head = PlainRecord::new(&identity, &value).head(store.state.record_key, place);
    claim.data.write_at(claim.offset, &[IoSlice::new(&head), IoSlice::new(&value)]).expect("write");
    let row = WriteRow { place, offset: claim.offset, length, kind: RowKind::Value, slot: claim.slot, segment: claim.segment, has_newer_sealed: false };
    assert!(matches!(store.state.land_reserved(BULK, key, &row, Some(&value)), LandOutcome::Landed));
    drop(room);
    claim.land();
    // the seal notes its place when the footer starts, and the install later raises it to the footer's own, so look in between
    let sealed_before = bulk.highest_sealed.load(Ordering::Acquire);
    for _step in 0..STEPS {
        if bulk.highest_sealed.load(Ordering::Acquire) != sealed_before {
            break;
        }
        if io.step_any().is_none() {
            io.advance_clock(TICK);
        }
    }
    assert_eq!(bulk.seals_done.load(Ordering::Acquire), 0, "the seal installed before its first noted place could be read");
    assert!(bulk.highest_sealed.load(Ordering::Acquire) >= place.0, "the seal notes a place below the late row's {place:?}");
    settle(&io);

    assert_eq!(bulk.seals_done.load(Ordering::Acquire), 1);
    assert!(bulk.highest_sealed.load(Ordering::Acquire) >= place.0, "the sealed place stops below the late row's {place:?}");
    let mut buf = Vec::new();
    assert!(store.get(BULK, key, &mut buf).expect("get"), "the late row is gone after the seal");
    assert_eq!(buf, value);
    let mut scan = store.iter_from(BULK, key, reel_core::direction::Direction::Ascending).expect("scan");
    let mut name = Vec::new();
    assert_eq!(scan.next_key(&mut name).expect("step"), reel_core::scan::ScanStep::Found);
    assert_eq!(name, key);
    drop(scan);
    store.close().expect("close");

    let store = ReelStore::open(io.clone(), &config).expect("reopen");
    buf.clear();
    assert!(store.get(BULK, key, &mut buf).expect("get"), "the late row is gone after a reopen");
    assert_eq!(buf, value);
    for number in 0..earlier_rows {
        assert!(store.get(BULK, &number.to_be_bytes(), &mut buf).expect("get"));
    }
    store.close().expect("close");
}

#[test]
fn a_late_claim_under_a_seal_lands_in_its_footer() {
    seal_with_a_late_claim(100);
}

#[test]
fn a_late_claim_alone_in_its_segment_gives_the_footer_its_place_base() {
    seal_with_a_late_claim(0);
}

const SQUEEZED: ColumnId = ColumnId(3);

fn noise(seed: u64, length: usize) -> Vec<u8> {
    let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    (0..length)
        .map(|_at| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state as u8
        })
        .collect()
}

fn mixed_config() -> StoreConfig {
    StoreConfig {
        memory_budget: ByteCount(2 * BUDGET.0),
        threads: reel::config::NO_WORKERS,
        volumes: vec![volume("bulk", BULK_ROOT), volume("meta", META_ROOT)],
        columns: vec![
            column(BULK, "bulk", 0),
            column(META, "meta", 1),
            ColumnConfig { codec: Codec::Lz4, ..column(SQUEEZED, "squeezed", 1) },
        ],
    }
}

// a scan of rows with their values first, so it reads the records before any get takes them into the cache, then a get of each
fn check_all(store: &Store2, column: ColumnId, model: &[(Vec<u8>, Vec<u8>)], when: &str) {
    let mut scan = store.iter_from(column, &[], reel_core::direction::Direction::Ascending).expect("scan");
    let (mut name, mut value, mut found) = (Vec::new(), Vec::new(), Vec::new());
    while scan.next_row(&mut name, &mut value).expect("step") == reel_core::scan::ScanStep::Found {
        found.push((name.clone(), value.clone()));
    }
    drop(scan);
    let mut sorted = model.to_vec();
    sorted.sort();
    assert_eq!(found, sorted, "{when}: a scan");
    for (key, value) in model {
        assert_eq!(read(store, column, key).as_deref(), Some(value.as_slice()), "{when}: a get of {key:?}");
    }
}

// a small value on a compressed column lands in the window as a plain one does, and reads back from there, after its seal and
// after a reopen, both as it shrank and as it was written
#[test]
fn small_compressed_values_land_in_the_window() {
    let io = SimulatedIo::new(Seed(21));
    let config = mixed_config();
    let store = ReelStore::open(io.clone(), &config).expect("open");
    let model: Vec<(Vec<u8>, Vec<u8>)> = (0..40_u64)
        .map(|number| {
            let value = if number % 2 == 0 { vec![number as u8; 600 + 10 * number as usize] } else { noise(number, 600 + 10 * number as usize) };
            (number.to_be_bytes().to_vec(), value)
        })
        .collect();
    let windowed = store.state.scan_window_records.load(Ordering::Relaxed);
    for (key, value) in &model {
        store.put(SQUEEZED, key, value).expect("put");
    }
    let held: usize = store.state.windows.iter().flatten().map(|window| window.used()).sum();
    let stored: usize = model.iter().map(|(_key, value)| value.len()).sum();
    assert!(held >= stored / 4, "the windows hold {held} bytes of {stored} written");
    check_all(&store, SQUEEZED, &model, "in the window");
    assert!(store.state.scan_window_records.load(Ordering::Relaxed) > windowed, "the scan read no record from the window");
    store.seal_now();
    settle(&io);
    check_all(&store, SQUEEZED, &model, "after the seal");
    store.close().expect("close");
    let store = ReelStore::open(io.clone(), &config).expect("reopen");
    check_all(&store, SQUEEZED, &model, "after a reopen");
    store.close().expect("close");
}

// a large record writes its body, then its places, then its head page, and reads back whole wherever the cut falls: past a plain
// value's head, inside a chunked record's table, as written and compressed
#[test]
fn large_records_split_at_their_head_page_read_back() {
    let io = SimulatedIo::new(Seed(22));
    let config = mixed_config();
    let store = ReelStore::open(io.clone(), &config).expect("open");
    let plain: Vec<(Vec<u8>, Vec<u8>)> = [65_527_usize, 65_528, 65_536, 200_000]
        .iter()
        .enumerate()
        .map(|(at, &length)| (format!("plain-{length}").into_bytes(), noise(at as u64 + 1, length)))
        .collect();
    let squeezed: Vec<(Vec<u8>, Vec<u8>)> = vec![
        (b"shrinks".to_vec(), vec![3; 70_000]),
        (b"written".to_vec(), noise(9, 70_000)),
        (b"wide".to_vec(), noise(10, 300_000)),
    ];
    for (key, value) in plain.iter() {
        store.put(BULK, key, value).expect("put");
    }
    for (key, value) in squeezed.iter() {
        store.put(SQUEEZED, key, value).expect("put");
    }
    check_all(&store, BULK, &plain, "open");
    check_all(&store, SQUEEZED, &squeezed, "open");
    store.seal_now();
    settle(&io);
    check_all(&store, BULK, &plain, "after the seal");
    check_all(&store, SQUEEZED, &squeezed, "after the seal");
    store.close().expect("close");
    let store = ReelStore::open(io.clone(), &config).expect("reopen");
    check_all(&store, BULK, &plain, "after a reopen");
    check_all(&store, SQUEEZED, &squeezed, "after a reopen");
    store.close().expect("close");
}

// a kill after a late record's body is written and before its head is leaves a range with no row: the rows around it read back,
// the store takes writes and seals, and the key never shows
#[test]
fn a_kill_between_a_late_body_and_its_head_leaves_no_row() {
    let io = SimulatedIo::new(Seed(23));
    let config = mixed_config();
    let store = ReelStore::open(io.clone(), &config).expect("open");
    let bulk = &store.state.volumes[0];
    let mut model: Vec<(Vec<u8>, Vec<u8>)> = (0..20_u64).map(|number| (number.to_be_bytes().to_vec(), noise(number + 1, 5_000))).collect();
    for (key, value) in &model {
        store.put(BULK, key, value).expect("put");
    }

    let value = noise(77, 100_000);
    let length = plain_length(ByteCount(value.len() as u64));
    let request = ClaimRequest { bytes: length, has_records: true, ..ONE_ROW };
    let (claim, room) = store.state.claim_room_late(bulk, &request, row_size(b"torn".len()), true).expect("claim");
    let identity = RecordIdentity { record_key: store.state.record_key, column: BULK, key: b"torn", kind: RowKind::Value, place: WritePlace(0) };
    let head = PlainRecord::new(&identity, &value).head(store.state.record_key, WritePlace(0));
    let parts = [IoSlice::new(&head), IoSlice::new(&value)];
    reel::record::record_encoder::write_parts_range(&claim.data, claim.offset, &parts, (HEAD_PAGE_BYTES, length.0 as usize)).expect("body");
    // a writer after it lands past the torn range
    let after = (b"after".to_vec(), noise(99, 5_000));
    store.put(BULK, &after.0, &after.1).expect("put");
    model.push(after);
    store.flush().expect("flush");
    io.kill();
    std::mem::forget(claim);
    std::mem::forget(room);
    drop(store);

    let store = ReelStore::open(io.clone(), &config).expect("reopen");
    check_all(&store, BULK, &model, "after the kill");
    assert_eq!(read(&store, BULK, b"torn"), None);
    let later = (b"later".to_vec(), noise(100, 5_000));
    store.put(BULK, &later.0, &later.1).expect("put");
    model.push(later);
    store.seal_now();
    settle(&io);
    check_all(&store, BULK, &model, "after a seal past the torn range");
    store.close().expect("close");
    let store = ReelStore::open(io.clone(), &config).expect("reopen");
    check_all(&store, BULK, &model, "after the last reopen");
    assert_eq!(read(&store, BULK, b"torn"), None);
    store.close().expect("close");
}

const SWEEP_POINTS: u64 = 60;

// a crash at each operation of one late put, its body, its head and its row's send, under no syncs and under a policy that syncs
// inside the put: the earlier rows read, the late key is gone or whole, and the store takes a put and a seal after
fn crash_through_a_late_put(sync: SyncPolicy, power_cut: bool) {
    let mut has_finished = false;
    for crash_at in 0..SWEEP_POINTS {
        let io = SimulatedIo::new(Seed(crash_at + 31));
        let mut config = mixed_config();
        for volume in &mut config.volumes {
            volume.sync = sync;
        }
        let store = ReelStore::open(io.clone(), &config).expect("open");
        let earlier: Vec<(Vec<u8>, Vec<u8>)> = (0..12_u64).map(|number| (number.to_be_bytes().to_vec(), noise(number + 1, 3_000))).collect();
        for (key, value) in &earlier {
            store.put(BULK, key, value).expect("put");
        }
        store.flush().expect("flush");
        let late = noise(500, 200_000);
        io.arm_crash(OperationCount(crash_at));
        let landed = store.put(BULK, b"late", &late).is_ok() && !io.is_crashed();
        io.disarm_crash();
        if power_cut {
            io.power_cut();
        } else {
            io.kill();
        }
        drop(store);

        let store = ReelStore::open(io.clone(), &config).expect("reopen");
        check_all(&store, BULK, &earlier_with(&earlier, &store), "after the crash");
        match read(&store, BULK, b"late") {
            Some(found) => assert_eq!(found, late, "crash at {crash_at}: the late value came back torn"),
            None => assert!(!landed || sync == SyncPolicy::Never, "crash at {crash_at}: a put that synced itself and returned is gone"),
        }
        store.put(BULK, b"after", b"value").expect("put");
        store.seal_now();
        settle(&io);
        assert_eq!(read(&store, BULK, b"after").as_deref(), Some(&b"value"[..]), "crash at {crash_at}");
        store.close().expect("close");
        assert_eq!(io.open_handle_count(), 0, "crash at {crash_at}");
        assert_eq!(io.handle_faults(), OperationCount(0), "crash at {crash_at}");
        has_finished |= landed;
    }
    assert!(has_finished, "no crash point let the late put finish, so the sweep never passed its last operation");
}

// the earlier rows, plus the late key when it came back, so the scan in check_all lists exactly what the store holds
fn earlier_with(earlier: &[(Vec<u8>, Vec<u8>)], store: &Store2) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut rows = earlier.to_vec();
    if let Some(late) = read(store, BULK, b"late") {
        rows.push((b"late".to_vec(), late));
    }
    rows
}

#[test]
fn a_crash_through_a_late_put_with_no_syncs() {
    crash_through_a_late_put(SyncPolicy::Never, false);
}

#[test]
fn a_crash_through_a_late_put_that_syncs_inside() {
    crash_through_a_late_put(SyncPolicy::EveryBytes(ByteCount(64 * 1024)), false);
}

#[test]
fn a_power_cut_through_a_late_put_that_syncs_inside() {
    crash_through_a_late_put(SyncPolicy::EveryBytes(ByteCount(64 * 1024)), true);
}
