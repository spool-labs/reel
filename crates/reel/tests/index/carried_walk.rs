//! Values carried resident for a column whose ceiling passes the entry array
//!
//! The slot-meta shape: a few hundred bytes per key, rewritten while hot, walked in
//! key order. The carried map serves the walk and the point read from memory, a value
//! past the ceiling reads from the volume, and a reopened store starts cold. Device
//! reads are counted through the simulator, so "from memory" is a measured fact.

use std::path::PathBuf;
use std::sync::Arc;

use reel_core::Store;

use reel::io::fault::FaultPlan;
use reel::io::sim_backend::SimIo;
use reel::{
    ByteCount, Codec, ColumnId, ColumnSet, ColumnSpec, IndexResidency, KeyWidth, MapShape,
    Preallocate, ReelConfig, ReelStore, SyncPolicy, ThreadBudget,
};

const SLOTS: u64 = 256;
const META_LEN: usize = 300;
const CEILING: u16 = 1024;
const FILLER_LEN: usize = 38 * 1024;

const fn meta(inline_max: u16, codec: Codec) -> ColumnSpec {
    ColumnSpec {
        id: ColumnId(1),
        name: "meta",
        key_width: KeyWidth::Fixed(8),
        shard_bytes: 0,
        inline_max,
        row_carry: 0,
        purge_mark: None,
        codec,
        map_shape: MapShape::Tree,
    }
}

const fn filler() -> ColumnSpec {
    ColumnSpec {
        id: ColumnId(2),
        name: "filler",
        key_width: KeyWidth::Fixed(8),
        shard_bytes: 0,
        inline_max: 0,
        row_carry: 0,
        purge_mark: None,
        codec: Codec::None,
        map_shape: MapShape::Tree,
    }
}

const CARRYING: ColumnSet = &[meta(CEILING, Codec::None), filler()];

fn config() -> ReelConfig {
    ReelConfig {
        segment_bytes: ByteCount::from_bytes(64 * 1024 * 1024),
        alloc_chunk: ByteCount::from_bytes(1024 * 1024),
        preallocate: Preallocate::Chunk,
        sync: SyncPolicy::Never,
        active_tails: ThreadBudget::threads(1),
        ..ReelConfig::default()
    }
}

fn value(slot: u64, len: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(len);
    let mut state = slot | 1;
    while out.len() < len {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        out.extend_from_slice(&state.to_le_bytes());
    }
    out.truncate(len);
    out
}

/// The slot-meta geometry: metas scattered between filler far past the merge gap
fn filled() -> (ReelStore, SimIo) {
    let sim = SimIo::new(FaultPlan::new(17));
    let store = ReelStore::open_with_io(
        PathBuf::from("/carried"),
        config(),
        CARRYING,
        Arc::new(sim.clone()),
    )
    .expect("open");
    let filler_value = vec![0xCDu8; FILLER_LEN];
    for slot in 0..SLOTS {
        let key = slot.to_be_bytes();
        Store::put(&store, "meta", &key, &value(slot, META_LEN)).expect("meta");
        Store::put(&store, "filler", &key, &filler_value).expect("filler");
    }
    (store, sim)
}

// the ordered walk serves every carried value without touching the device
#[test]
fn a_walk_over_a_carrying_column_reads_nothing() {
    let (store, sim) = filled();
    let before = sim.read_count();
    let mut rows = 0u64;
    for (key, found) in Store::iter(&store, "meta").expect("iter") {
        let slot = u64::from_be_bytes(key.as_slice().try_into().expect("width"));
        assert_eq!(&*found, &value(slot, META_LEN)[..], "walk at {slot}");
        rows += 1;
    }
    assert_eq!(rows, SLOTS);
    assert_eq!(sim.read_count() - before, 0, "the walk went to the device");
}

// a point read is a map lookup, and an overwrite is never served stale
#[test]
fn point_reads_serve_the_newest_value_from_memory() {
    let (store, sim) = filled();
    let key = 7u64.to_be_bytes();
    let rewritten = value(7_000_000, META_LEN);
    Store::put(&store, "meta", &key, &rewritten).expect("overwrite");

    let before = sim.read_count();
    let found = Store::get(&store, "meta", &key)
        .expect("get")
        .expect("present");
    assert_eq!(&*found, &rewritten[..]);
    assert_eq!(
        sim.read_count() - before,
        0,
        "the point read went to the device"
    );
}

// a deleted key stays gone, walk and point alike
#[test]
fn a_delete_evicts_the_carried_value() {
    let (store, _sim) = filled();
    let key = 9u64.to_be_bytes();
    Store::delete(&store, "meta", &key).expect("delete");

    assert!(Store::get(&store, "meta", &key).expect("get").is_none());
    let walked: Vec<u64> = Store::iter(&store, "meta")
        .expect("iter")
        .map(|(key, _)| u64::from_be_bytes(key.as_slice().try_into().expect("width")))
        .collect();
    assert_eq!(walked.len() as u64, SLOTS - 1);
    assert!(!walked.contains(&9));
}

// a value past the ceiling reads from the volume and still answers whole
#[test]
fn an_oversize_value_reads_from_the_device() {
    let (store, sim) = filled();
    let key = 300u64.to_be_bytes();
    let oversize = value(300, CEILING as usize * 2);
    Store::put(&store, "meta", &key, &oversize).expect("put");

    let before = sim.read_count();
    let found = Store::get(&store, "meta", &key)
        .expect("get")
        .expect("present");
    assert_eq!(&*found, &oversize[..]);
    assert!(
        sim.read_count() > before,
        "an oversize value cannot be carried"
    );
}

// a reopened store starts cold and warms itself: pay the device once, then never
#[test]
fn a_reopen_warms_on_first_read() {
    let (store, sim) = filled();
    store.flush().expect("flush");
    drop(store);

    let restored = SimIo::from_image(sim.durable_image());
    let store = ReelStore::open_with_io(
        PathBuf::from("/carried"),
        config(),
        CARRYING,
        Arc::new(restored.clone()),
    )
    .expect("reopen");

    let key = 42u64.to_be_bytes();
    let cold = restored.read_count();
    let found = Store::get(&store, "meta", &key)
        .expect("get")
        .expect("present");
    assert_eq!(&*found, &value(42, META_LEN)[..]);
    assert!(restored.read_count() > cold, "a reopened store starts cold");

    let warm = restored.read_count();
    let found = Store::get(&store, "meta", &key)
        .expect("get")
        .expect("present");
    assert_eq!(&*found, &value(42, META_LEN)[..]);
    assert_eq!(
        restored.read_count(),
        warm,
        "the second read paid the device again"
    );
}

// a carrying column cannot page and cannot compress, said at open rather than later
#[test]
fn carrying_rejects_paging_and_codecs() {
    let sim = SimIo::new(FaultPlan::new(19));
    let paged = ReelStore::open_with_io(
        PathBuf::from("/rejects"),
        ReelConfig {
            index: IndexResidency::Paged,
            ..config()
        },
        CARRYING,
        Arc::new(sim.clone()),
    );
    assert!(paged.is_err(), "a paged index cannot hold carried values");

    const CODED: ColumnSet = &[meta(CEILING, Codec::Lz4), filler()];
    let coded = ReelStore::open_with_io(PathBuf::from("/rejects"), config(), CODED, Arc::new(sim));
    assert!(
        coded.is_err(),
        "carried values cannot disagree with stored bytes"
    );
}
