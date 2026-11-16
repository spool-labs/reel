//! Death marks: records that die together sit together, and their segments go whole

use std::path::PathBuf;
use std::sync::atomic::Ordering;

use reel::config::{ColumnConfig, ColumnIndex, KeyShape, StoreConfig, SyncPolicy, VolumeConfig};
use reel::engine::open_segment::SegmentPhase;
use reel::engine::reel_store::ReelStore;
use reel::guard::hold;
use reel::lifetime::{MOST_LIFETIME_GROUPS, MarkRange};
use reel::engine::rewrite_job::MOST_OVERLAP;
use reel::record::codec::Codec;
use reel::segment::segment_tail::SegmentNumber;
use reel_core::column_id::ColumnId;
use reel_core::maintenance_report::{MaintenanceReport, VolumeReading};
use reel_core::units::{ByteCount, DeathMark, Nanoseconds, VolumeId};
use reel_core::write_batch::WriteBatch;
use reel_sim::seeded_random::Seed;
use reel_sim::simulated_io::SimulatedIo;

const ROOT: &str = "/disk/marks";
const PLANNER_JOB: &str = "reel-planner";
const COLUMN: ColumnId = ColumnId(1);
const BUDGET: ByteCount = ByteCount(64 * 1024 * 1024);
const TICK: Nanoseconds = Nanoseconds(1_000_000);
const IDLE_LOOK: Nanoseconds = Nanoseconds(1_000_000_000);
const IDLE_TICKS: u64 = 8;
const STEPS: u64 = 20_000;
const VALUE_BYTES: usize = 1_000;
const KEYS: u64 = 100;
const LIFETIMES: u64 = 6;
const LIFETIME_KEYS: u64 = 400;
const FAR_APART: u64 = 1_000_000;

type Store = ReelStore<SimulatedIo>;

fn config() -> StoreConfig {
    StoreConfig {
        memory_budget: BUDGET,
        threads: reel::config::NO_WORKERS,
        volumes: vec![VolumeConfig {
            name: "marks".to_string(),
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

fn idle(store: &Store, io: &SimulatedIo) {
    for _tick in 0..IDLE_TICKS {
        io.advance_clock(IDLE_LOOK);
        store.state.ask_compaction();
        settle(io);
    }
}

fn reading(store: &Store) -> VolumeReading {
    let mut report = MaintenanceReport::default();
    store.maintenance_report(&mut report);
    report.volumes[0]
}

fn key(number: u64) -> Vec<u8> {
    format!("key-{number:06}").into_bytes()
}

fn put_keys(store: &Store, first: u64, count: u64, mark: DeathMark) {
    let value = vec![7; VALUE_BYTES];
    for number in first..first + count {
        store.put_marked(COLUMN, &key(number), &value, mark).expect("put");
    }
}

fn delete_keys(store: &Store, first: u64, count: u64) {
    for number in first..first + count {
        store.delete(COLUMN, &key(number)).expect("delete");
    }
}

fn open_ceilings(store: &Store) -> Vec<Option<DeathMark>> {
    let claims = hold(&store.state.volumes[0].claims);
    let taking = claims.open.iter().filter(|segment| segment.phase == SegmentPhase::Taking && segment.row_count.0 > 0);
    taking.map(|segment| segment.ceiling).collect()
}

fn seal(store: &Store, io: &SimulatedIo) {
    store.seal_now();
    settle(io);
}

fn seal_all(store: &Store, io: &SimulatedIo) {
    while !open_ceilings(store).is_empty() {
        seal(store, io);
    }
}

fn sealed_marks(store: &Store) -> Vec<(SegmentNumber, MarkRange)> {
    let rows = hold(&store.state.volumes[0].summary);
    rows.rows().map(|row| (row.facts.segment, row.facts.marks)).collect()
}

// nearby marks share a segment, and an unmarked put keeps out of it
#[test]
fn nearby_marks_share_a_group() {
    let io = SimulatedIo::new(Seed(1));
    let store = open(&io);
    store.put_marked(COLUMN, b"a", b"value", DeathMark(1_000)).expect("put");
    store.put_marked(COLUMN, b"b", b"value", DeathMark(1_010)).expect("put");
    assert_eq!(open_ceilings(&store).len(), 1);
    store.put(COLUMN, b"c", b"value").expect("put");
    let ceilings = open_ceilings(&store);
    assert_eq!(ceilings.len(), 2);
    assert!(ceilings.contains(&None));
    assert_eq!(reading(&store).lifetime_groups, 1);
    store.close().expect("close");
}

// a long-lived mark doesn't take the short ones in, or one group would end up with everything
#[test]
fn long_mark_keeps_short_marks_out() {
    let io = SimulatedIo::new(Seed(2));
    let store = open(&io);
    store.put_marked(COLUMN, b"a", b"value", DeathMark(FAR_APART)).expect("put");
    store.put_marked(COLUMN, b"b", b"value", DeathMark(10)).expect("put");
    assert_eq!(reading(&store).lifetime_groups, 2);
    store.close().expect("close");
}

// a mark takes the lowest ceiling it may join
#[test]
fn lowest_ceiling_wins() {
    let io = SimulatedIo::new(Seed(3));
    let store = open(&io);
    store.put_marked(COLUMN, b"a", b"value", DeathMark(1_000)).expect("put");
    store.put_marked(COLUMN, b"b", b"value", DeathMark(1_200)).expect("put");
    let before = open_ceilings(&store);
    store.put_marked(COLUMN, b"c", b"value", DeathMark(990)).expect("put");
    assert_eq!(open_ceilings(&store), before);
    let lowest = before.iter().flatten().min().copied();
    let claims = hold(&store.state.volumes[0].claims);
    let home = claims.open.iter().find(|segment| segment.ceiling == lowest).expect("group");
    assert_eq!(home.marks.lowest, DeathMark(990));
    drop(claims);
    store.close().expect("close");
}

// a mark with no life left joins only its exact ceiling
#[test]
fn spent_mark_needs_its_ceiling() {
    let io = SimulatedIo::new(Seed(4));
    let store = open(&io);
    store.set_current_mark(DeathMark(100));
    store.put_marked(COLUMN, b"a", b"value", DeathMark(50)).expect("put");
    store.put_marked(COLUMN, b"b", b"value", DeathMark(50)).expect("put");
    assert_eq!(reading(&store).lifetime_groups, 1);
    store.put_marked(COLUMN, b"c", b"value", DeathMark(49)).expect("put");
    assert_eq!(reading(&store).lifetime_groups, 2);
    store.close().expect("close");
}

// with every group open, the group that dies first seals to make room
#[test]
fn first_to_die_makes_room() {
    let io = SimulatedIo::new(Seed(5));
    let store = open(&io);
    let far = |group: u64| DeathMark((group + 1) * FAR_APART);
    for group in 0..MOST_LIFETIME_GROUPS as u64 {
        store.put_marked(COLUMN, &key(group), b"value", far(group)).expect("put");
    }
    assert_eq!(reading(&store).lifetime_groups, MOST_LIFETIME_GROUPS);
    store.put_marked(COLUMN, b"late", b"value", DeathMark(100 * FAR_APART)).expect("put");
    settle(&io);
    assert_eq!(reading(&store).lifetime_groups, MOST_LIFETIME_GROUPS);
    let sealed = sealed_marks(&store);
    assert_eq!(sealed.len(), 1);
    assert_eq!(sealed[0].1.highest, far(0));
    store.close().expect("close");
}

// the current mark seals a group it has reached, and leaves the rest
#[test]
fn passed_group_seals() {
    let io = SimulatedIo::new(Seed(6));
    let store = open(&io);
    store.put_marked(COLUMN, b"a", b"value", DeathMark(1_000)).expect("put");
    store.put_marked(COLUMN, b"b", b"value", DeathMark(FAR_APART)).expect("put");
    store.set_current_mark(DeathMark(100));
    settle(&io);
    assert_eq!(reading(&store).lifetime_groups, 2);
    store.set_current_mark(DeathMark(2_000));
    settle(&io);
    assert_eq!(reading(&store).lifetime_groups, 1);
    assert_eq!(sealed_marks(&store).len(), 1);
    store.close().expect("close");
}

// a batch lands whole in its mark's group
#[test]
fn batch_takes_its_mark() {
    let io = SimulatedIo::new(Seed(7));
    let store = open(&io);
    let mut batch = WriteBatch::new();
    batch.set_mark(DeathMark(1_000));
    batch.put(COLUMN, b"a", b"value");
    batch.put(COLUMN, b"b", b"value");
    store.write_batch(&batch).expect("batch");
    assert_eq!(reading(&store).lifetime_groups, 1);
    assert_eq!(reading(&store).marks_dropped, 0);
    batch.clear();
    assert_eq!(batch.mark(), None);
    store.close().expect("close");
}

// a grave carries no mark and a batch never splits, so a marked batch with a delete goes unmarked and the report says so
#[test]
fn batch_with_a_delete_drops_its_mark() {
    let io = SimulatedIo::new(Seed(8));
    let store = open(&io);
    let mut batch = WriteBatch::new();
    batch.set_mark(DeathMark(1_000));
    batch.put(COLUMN, b"a", b"value");
    batch.delete(COLUMN, b"b");
    store.write_batch(&batch).expect("batch");
    let reading = reading(&store);
    assert_eq!((reading.lifetime_groups, reading.marks_dropped), (0, 1));
    assert_eq!(open_ceilings(&store), [None]);
    store.close().expect("close");
}

// a grave goes to the unmarked segments, so it never holds a marked segment back from its unlink
#[test]
fn grave_goes_unmarked() {
    let io = SimulatedIo::new(Seed(9));
    let store = open(&io);
    store.put_marked(COLUMN, b"a", b"value", DeathMark(1_000)).expect("put");
    store.delete(COLUMN, b"a").expect("delete");
    let ceilings = open_ceilings(&store);
    assert_eq!(ceilings.len(), 2);
    assert!(ceilings.contains(&None));
    store.close().expect("close");
}

// an overwrite with another mark is an ordinary overwrite
#[test]
fn overwrite_moves_lifetimes() {
    let io = SimulatedIo::new(Seed(10));
    let store = open(&io);
    store.put_marked(COLUMN, b"a", b"short", DeathMark(1_000)).expect("put");
    store.put_marked(COLUMN, b"a", b"long", DeathMark(FAR_APART)).expect("put");
    seal(&store, &io);
    let mut value = Vec::new();
    assert!(store.get(COLUMN, b"a", &mut value).expect("get"));
    assert_eq!(value, b"long");
    store.close().expect("close");
}

// a seal keeps the lowest and highest mark in the summary row, and a close and open keep them
#[test]
fn seal_keeps_the_range() {
    let io = SimulatedIo::new(Seed(11));
    let store = open(&io);
    store.put_marked(COLUMN, b"a", b"value", DeathMark(1_000)).expect("put");
    store.put_marked(COLUMN, b"b", b"value", DeathMark(990)).expect("put");
    seal(&store, &io);
    let expected = MarkRange { lowest: DeathMark(990), highest: DeathMark(1_000) };
    assert_eq!(sealed_marks(&store)[0].1, expected);
    assert_eq!(reading(&store).marked_seals, 1);
    assert!(reading(&store).marked_seal_bytes.0 > 0);
    store.close().expect("close");
    let store = open(&io);
    assert_eq!(sealed_marks(&store)[0].1, expected);
    store.close().expect("close");
}

// an open segment finds its group again from its rows after a crash, and the next record of that lifetime joins it
#[test]
fn crash_keeps_the_group() {
    let io = SimulatedIo::new(Seed(12));
    let store = open(&io);
    store.put_marked(COLUMN, b"a", b"value", DeathMark(1_000)).expect("put");
    store.put_marked(COLUMN, b"b", b"value", DeathMark(1_020)).expect("put");
    store.flush().expect("flush");
    settle(&io);
    io.power_cut();
    drop(store);
    let store = open(&io);
    assert_eq!(open_ceilings(&store), [Some(DeathMark(1_020))]);
    store.put_marked(COLUMN, b"c", b"value", DeathMark(1_020)).expect("put");
    assert_eq!(reading(&store).lifetime_groups, 1);
    let mut value = Vec::new();
    assert!(store.get(COLUMN, b"a", &mut value).expect("get"));
    store.close().expect("close");
}

fn marked_segment_dead(dead_keys: u64, current: DeathMark) -> (Store, SimulatedIo) {
    let io = SimulatedIo::new(Seed(20 + dead_keys));
    let store = open(&io);
    store.set_current_mark(current);
    put_keys(&store, 0, KEYS, DeathMark(FAR_APART));
    seal(&store, &io);
    delete_keys(&store, 0, dead_keys);
    seal(&store, &io);
    idle(&store, &io);
    (store, io)
}

// a marked segment 60% dead waits while its highest mark stands, and one 80% dead goes before its mark passes, so early deaths
// don't wait for the unlink
#[test]
fn early_bar() {
    // (tenths dead, early and plain bar rewrites)
    for (tenths, rewrites) in [(6, (0, 0)), (8, (1, 0))] {
        let (store, _io) = marked_segment_dead(KEYS * tenths / 10, DeathMark(0));
        let reading = reading(&store);
        assert_eq!((reading.early_bar_rewrites, reading.plain_bar_rewrites), rewrites, "{tenths} tenths dead");
        store.close().expect("close");
    }
}

// the 60% dead segment goes at the plain bar once the current mark has passed its highest
#[test]
fn plain_bar_after_the_mark() {
    let (store, io) = marked_segment_dead(KEYS * 6 / 10, DeathMark(0));
    store.set_current_mark(DeathMark(FAR_APART + 1));
    settle(&io);
    let reading = reading(&store);
    assert_eq!((reading.early_bar_rewrites, reading.plain_bar_rewrites), (0, 1));
    store.close().expect("close");
}

// a rewrite never joins two lifetimes, so its output still dies whole
#[test]
fn rewrite_keeps_one_lifetime() {
    let io = SimulatedIo::new(Seed(40));
    let store = open(&io);
    let (short, long) = (DeathMark(1_000), DeathMark(FAR_APART));
    put_keys(&store, 0, KEYS, short);
    seal(&store, &io);
    put_keys(&store, KEYS, KEYS, long);
    seal(&store, &io);
    delete_keys(&store, 0, KEYS * 9 / 10);
    delete_keys(&store, KEYS, KEYS * 9 / 10);
    seal(&store, &io);
    idle(&store, &io);
    assert_eq!(reading(&store).early_bar_rewrites, 2);
    let marked: Vec<_> = sealed_marks(&store).into_iter().map(|(_number, marks)| marks).filter(|marks| !marks.is_empty()).collect();
    assert_eq!(marked.len(), 2);
    assert!(marked.iter().all(|marks| marks.lowest == marks.highest));
    store.close().expect("close");
}

// lifetimes written side by side and deleted in turn leave by whole unlinks, with nothing copied
#[test]
fn lifetimes_leave_whole() {
    let io = SimulatedIo::new(Seed(41));
    let store = open(&io);
    let mark = |lifetime: u64| DeathMark((lifetime + 1) * FAR_APART);
    for number in 0..LIFETIME_KEYS {
        for lifetime in 0..LIFETIMES {
            put_keys(&store, lifetime * LIFETIME_KEYS + number, 1, mark(lifetime));
        }
    }
    for lifetime in 0..LIFETIMES {
        store.set_current_mark(mark(lifetime));
        delete_keys(&store, lifetime * LIFETIME_KEYS, LIFETIME_KEYS);
        seal_all(&store, &io);
        idle(&store, &io);
    }
    let mut report = MaintenanceReport::default();
    store.maintenance_report(&mut report);
    let reading = report.volumes[0];
    assert_eq!((reading.early_bar_rewrites, reading.plain_bar_rewrites), (0, 0), "{report:?}");
    assert!(report.segments_unlinked.0 >= LIFETIMES, "{report:?}");
    assert_eq!(reading.used_bytes, ByteCount(0), "{report:?}");
    store.close().expect("close");
}

// segments of many lifetimes over one key range pass the overlap bound and can't merge. A look that gives way to a waiting walk
// leaves the planner free to reach it, so the jobs go quiet
#[test]
fn crowd_of_lifetimes_goes_quiet() {
    let io = SimulatedIo::new(Seed(42));
    let store = open(&io);
    let put_lifetime = |lifetime: u64| {
        let mark = DeathMark((lifetime + 1) * FAR_APART);
        store.put_marked(COLUMN, format!("a-{lifetime:03}").as_bytes(), b"value", mark).expect("put");
        store.put_marked(COLUMN, format!("z-{lifetime:03}").as_bytes(), b"value", mark).expect("put");
    };
    let crowd = MOST_OVERLAP as u64 + 8;
    for lifetime in 0..crowd {
        put_lifetime(lifetime);
        seal_all(&store, &io);
    }

    io.hold_jobs(PLANNER_JOB, true);
    put_lifetime(crowd);
    seal_all(&store, &io);
    io.hold_jobs(PLANNER_JOB, false);
    store.state.volumes[0].has_walk_target.store(true, Ordering::Release);
    store.state.ask_compaction();
    settle(&io);

    assert!(sealed_marks(&store).len() > MOST_OVERLAP);
    assert!(!store.state.volumes[0].has_walk_target.load(Ordering::Acquire));
    store.close().expect("close");
}

// a range delete over a whole lifetime gives its space back on a quiet volume, with the delete itself the only unmarked row
#[test]
fn range_delete_ends_a_lifetime() {
    let io = SimulatedIo::new(Seed(43));
    let store = open(&io);
    put_keys(&store, 0, LIFETIME_KEYS, DeathMark(1_000));
    store.set_current_mark(DeathMark(2_000));
    settle(&io);
    assert_eq!(sealed_marks(&store).len(), 1);
    store.delete_range(COLUMN, &key(0), &key(LIFETIME_KEYS)).expect("delete");
    idle(&store, &io);
    let mut report = MaintenanceReport::default();
    store.maintenance_report(&mut report);
    assert!(report.segments_unlinked.0 >= 1, "{report:?}");
    assert_eq!(report.volumes[0].used_bytes, ByteCount(0), "{report:?}");
    store.close().expect("close");
}

// a volume set to two open segments holds two lifetime groups, and the group that dies first seals for a third
#[test]
fn open_segments_cap_the_groups() {
    let io = SimulatedIo::new(Seed(6));
    let mut config = config();
    config.volumes[0].open_segments = Some(2);
    let store = ReelStore::open(io.clone(), &config).expect("open");
    let far = |group: u64| DeathMark((group + 1) * FAR_APART);
    for group in 0..2 {
        store.put_marked(COLUMN, &key(group), b"value", far(group)).expect("put");
    }
    assert_eq!(reading(&store).lifetime_groups, 2);
    store.put_marked(COLUMN, b"late", b"value", DeathMark(100 * FAR_APART)).expect("put");
    settle(&io);
    assert_eq!(reading(&store).lifetime_groups, 2);
    let sealed = sealed_marks(&store);
    assert_eq!(sealed.len(), 1);
    assert_eq!(sealed[0].1.highest, far(0));
    store.close().expect("close");
}

// a volume set to no lifetime group refuses a marked write by name, a marked batch too, takes unmarked ones, and holds the slots
// of its open segment alone
#[test]
fn no_lifetime_groups_refuse_a_mark() {
    use reel_core::store_error::StoreError;
    let io = SimulatedIo::new(Seed(7));
    let mut config = config();
    config.volumes[0].open_segments = Some(1);
    config.volumes[0].lifetime_groups = Some(0);
    let store = ReelStore::open(io.clone(), &config).expect("open");
    let refused = store.put_marked(COLUMN, b"marked", b"value", DeathMark(FAR_APART));
    assert!(matches!(refused, Err(StoreError::NoLifetimeGroups { volume: VolumeId(0) })), "{refused:?}");
    let mut batch = WriteBatch::new();
    batch.set_mark(DeathMark(FAR_APART));
    batch.put(COLUMN, b"batched", b"value");
    let refused = store.write_batch(&batch);
    assert!(matches!(refused, Err(StoreError::NoLifetimeGroups { .. })), "{refused:?}");
    store.put(COLUMN, b"plain", b"value").expect("put");
    let mut value = Vec::new();
    assert!(store.get(COLUMN, b"plain", &mut value).expect("get"));
    assert!(!store.get(COLUMN, b"marked", &mut value).expect("get") && !store.get(COLUMN, b"batched", &mut value).expect("get"));
    // one open segment takes its own slot and one while it seals, and no group adds any
    assert_eq!(store.memory_report().open_slots, 2);
    store.close().expect("close");
}

// a store that held two open groups comes back with none: its marked rows read back, no group is open, and unmarked writes fill
// and seal segment after segment with no wait on a group nothing will seal, after a close and after a kill
#[test]
fn marked_rows_come_back_with_no_groups() {
    for seed in crate::seed_sets::seeds([8, 9]) {
        for is_killed in [false, true] {
            marked_rows_come_back(seed, is_killed);
        }
    }
}

fn marked_rows_come_back(seed: u64, is_killed: bool) {
    let io = SimulatedIo::new(Seed(seed));
    let store = open(&io);
    for number in 0..LIFETIME_KEYS {
        let mark = DeathMark((1 + number % 2) * 100 * FAR_APART);
        store.put_marked(COLUMN, &key(number), &[1; VALUE_BYTES], mark).expect("put");
    }
    settle(&io);
    assert_eq!(reading(&store).lifetime_groups, 2, "seed {seed}");
    if is_killed {
        store.flush().expect("flush");
        io.kill();
    } else {
        store.close().expect("close");
    }
    drop(store);

    // the same store reopened with groups on keeps its recovered groups open
    {
        let store = ReelStore::open(io.clone(), &config()).expect("reopen with groups");
        settle(&io);
        assert!(reading(&store).lifetime_groups >= 1, "seed {seed}, killed {is_killed}: a volume with groups sealed its recovered group");
        store.close().expect("close");
    }
    let mut none = config();
    none.volumes[0].open_segments = Some(1);
    none.volumes[0].lifetime_groups = Some(0);
    none.volumes[0].segment_bytes = Some(ByteCount(256 * 1024));
    let store = ReelStore::open(io.clone(), &none).expect("reopen");
    settle(&io);
    let mut value = Vec::new();
    for number in 0..LIFETIME_KEYS {
        assert!(store.get(COLUMN, &key(number), &mut value).expect("get"), "seed {seed}, killed {is_killed}: key {number}");
    }
    assert_eq!(reading(&store).lifetime_groups, 0, "seed {seed}, killed {is_killed}: a group is still open");
    {
        let state = slot_state(&store);
        let claims = hold(&store.state.volumes[0].claims);
        let free = hold(&store.state.free_slots).len();
        // every slot is an open segment, the spare or free: none is held by a group no write will fill
        let accounted = claims.open.len() + usize::from(claims.prepared.is_some()) + free;
        assert!(claims.open.iter().all(|open| open.ceiling.is_none()) && accounted == 2, "seed {seed}, killed {is_killed}: the slots don't add up: {state}");
    }
    let sealed_before = hold(&store.state.volumes[0].summary).rows().count();
    // the puts run on this thread, as every sim drives the store, so each pause a writer takes steps the jobs it waits for
    for number in 0..UNMARKED_KEYS {
        if let Err(error) = store.put(COLUMN, format!("plain-{number:05}").as_bytes(), &[2; VALUE_BYTES]) {
            panic!("seed {seed}, killed {is_killed}: put {number}: {error:?}; {}", slot_state(&store));
        }
    }
    settle(&io);
    let sealed = hold(&store.state.volumes[0].summary).rows().count() - sealed_before;
    assert!(sealed >= 4, "seed {seed}, killed {is_killed}: {sealed} segments sealed");
    for number in 0..LIFETIME_KEYS {
        assert!(store.get(COLUMN, &key(number), &mut value).expect("get"), "seed {seed}, killed {is_killed}: key {number} after the fill");
    }
    store.close().expect("close");
}

// unmarked keys of VALUE_BYTES that fill several 256 KiB segments
const UNMARKED_KEYS: u64 = 1_500;

// the volume's slot state for a failure message: the groups first, since the report takes the claims lock, then each open
// segment with its phase and slot, the spare, and the free list
fn slot_state(store: &Store) -> String {
    let groups = reading(store).lifetime_groups;
    let claims = hold(&store.state.volumes[0].claims);
    let open: Vec<String> = claims.open.iter().map(|segment| format!("{:?} slot {:?} {:?}", segment.number, segment.slot, segment.phase)).collect();
    let free = hold(&store.state.free_slots).clone();
    format!(
        "groups {groups}, open {open:?}, spare {:?}, creating {}, wants a spare {}, free {free:?}",
        claims.prepared.as_ref().map(|segment| (segment.number, segment.slot)),
        claims.is_creating,
        claims.wants_prepared
    )
}
