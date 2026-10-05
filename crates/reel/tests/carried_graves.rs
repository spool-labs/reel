//! A tombstone compaction carries keeps its key out of walks, and never hides a newer put
//!
//! On a paged volume a sealed tombstone has no grave in the map: a reopen rebuilds the
//! map from what is unsealed, and the prune gives a grave up once its segment is noted.
//! A pass that carries the tombstone retires that segment, so until the copy's own
//! segment is noted the copy stands as a grave in the map. That grave must never stand
//! over a version newer than the tombstone, wherever the newer version sits.

use tempfile::TempDir;

use reel::format::column::RecordKey;
use reel::format::loc::SegmentId;
use reel::format::lsn::Lsn;
use reel::{
    ByteCount, Codec, ColumnId, ColumnSet, ColumnSpec, IndexResidency, KeyWidth,
    ReelConfig, ReelStore, SyncPolicy, ThreadBudget,
};
use reel_core::{Direction, Store};

const COLUMNS: ColumnSet = &[ColumnSpec {
    id: ColumnId(1),
    name: "rows",
    key_width: KeyWidth::Fixed(16),
    shard_bytes: 0,
    purge_mark: None,
    codec: Codec::None,
}];

/// A segment no tail will reach, standing in for a carried copy's segment
const COPY: SegmentId = SegmentId(u32::MAX - 1);

/// Filler keys a round writes, enough to roll a segment more than once
const FILL: u32 = 400;

fn paged(dir: &TempDir) -> ReelStore {
    ReelStore::open(dir.path().to_path_buf(), config(), COLUMNS).expect("open")
}

fn config() -> ReelConfig {
    ReelConfig {
        segment_bytes: ByteCount::from_bytes(64 * 1024),
        alloc_chunk: ByteCount::from_bytes(16 * 1024),
        sync: SyncPolicy::Never,
        active_tails: ThreadBudget::threads(1),
        compact_dead_ratio: 0.5,
        scrub_mbps: 0,
        index: IndexResidency::Paged,
        ..ReelConfig::default()
    }
}

/// Close and open again, which leaves a sealed tombstone with no grave in the map
fn reopen(store: ReelStore, dir: &TempDir) -> ReelStore {
    store.close().expect("close");
    drop(store);
    paged(dir)
}

fn the_key() -> Vec<u8> {
    vec![7u8; 16]
}

fn filler(n: u32) -> Vec<u8> {
    let mut key = vec![0xEE];
    key.extend_from_slice(&n.to_be_bytes());
    key.resize(16, 0);
    key
}

/// Write a round's own fillers, then let maintenance note and hand over what sealed
///
/// Each round has keys of its own, so a round kills nothing an earlier one wrote.
fn fill(store: &ReelStore, round: u32) {
    write_round(store, round, round as u8);
}

/// Write an earlier round's fillers again, which leaves its segment mostly dead
fn overwrite(store: &ReelStore, round: u32, with: u8) {
    write_round(store, round, with);
}

fn write_round(store: &ReelStore, round: u32, with: u8) {
    write_only(store, round, with);
    for _ in 0..3 {
        Store::maintain(store).expect("maintain");
    }
}

/// Write a round's fillers with no maintenance after
fn write_only(store: &ReelStore, round: u32, with: u8) {
    for n in 0..FILL {
        Store::put(store, "rows", &filler(round * FILL + n), &[with; 200]).expect("filler");
    }
}

fn value(store: &ReelStore) -> Option<Vec<u8>> {
    Store::get(store, "rows", &the_key())
        .expect("get")
        .map(|value| value.to_vec())
}

fn walked(store: &ReelStore) -> bool {
    store
        .iter_keys_from("rows", None, Direction::Asc)
        .expect("keys")
        .any(|key| key == the_key())
}

fn record_key() -> RecordKey {
    RecordKey::from_bytes(ColumnId(1), &the_key()).expect("key")
}

#[test]
fn a_carried_tombstone_keeps_its_key_out_of_walks() {
    let dir = TempDir::new().expect("temp dir");
    let store = paged(&dir);
    // The first version seals with fillers that stay live, so its segment never compacts.
    Store::put(&store, "rows", &the_key(), b"first").expect("put");
    fill(&store, 1);
    // The tombstone seals beside fillers that are written again, so its segment does.
    Store::delete(&store, "rows", &the_key()).expect("delete");
    fill(&store, 2);
    let store = reopen(store, &dir);
    write_only(&store, 2, 3);
    // Walked right after the pass that carries it, before anything notes the copy's segment.
    let mut is_carried = false;
    for _ in 0..8 {
        let carried = store.compaction_counters().tombstones_carried;
        store.compact_once().expect("compact");
        // The walk goes first: a get notes the segments that sealed, which would close the window.
        if store.compaction_counters().tombstones_carried > carried {
            is_carried = true;
            assert!(!walked(&store), "a walk found the deleted key's first version");
            assert_eq!(value(&store), None, "a get found the deleted key");
        }
    }
    assert!(is_carried, "no pass carried the tombstone, so this proves nothing");
}

#[test]
fn a_grave_refuses_a_put_already_handed_over() {
    let dir = TempDir::new().expect("temp dir");
    let store = paged(&dir);
    Store::put(&store, "rows", &the_key(), b"first").expect("put");
    fill(&store, 1);
    Store::delete(&store, "rows", &the_key()).expect("delete");
    let deleted_at = store.sequence();
    Store::put(&store, "rows", &the_key(), b"second").expect("put again");
    fill(&store, 2);
    let column = store.index().column(ColumnId(1)).expect("column");
    assert!(column.entry_or_grave(&the_key()).is_none(), "the second put was never handed over");

    store.index().hold_grave(&record_key(), deleted_at, COPY);
    assert!(column.entry_or_grave(&the_key()).is_none(), "a grave stood over a newer put");
    assert_eq!(value(&store), Some(b"second".to_vec()));
    assert!(walked(&store), "a walk lost the second put");
}

#[test]
fn a_grave_stands_for_a_key_with_nothing_newer() {
    let dir = TempDir::new().expect("temp dir");
    let store = paged(&dir);
    Store::put(&store, "rows", &the_key(), b"first").expect("put");
    fill(&store, 1);
    Store::delete(&store, "rows", &the_key()).expect("delete");
    let deleted_at = store.sequence();
    fill(&store, 2);

    store.index().hold_grave(&record_key(), deleted_at, COPY);
    let held = store
        .index()
        .column(ColumnId(1))
        .expect("column")
        .entry_or_grave(&the_key());
    assert!(held.is_some_and(|entry| entry.is_grave()), "no grave stood for the deleted key");
    assert_eq!(value(&store), None);
    assert!(!walked(&store), "a walk found the deleted key");
}

#[test]
fn a_put_after_a_delete_survives_compacting_its_tombstone() {
    let dir = TempDir::new().expect("temp dir");
    let store = paged(&dir);
    Store::put(&store, "rows", &the_key(), b"first").expect("put");
    fill(&store, 1);
    Store::delete(&store, "rows", &the_key()).expect("delete");
    fill(&store, 2);
    // The second version seals and is handed over, so only FastForward holds it.
    Store::put(&store, "rows", &the_key(), b"second").expect("put again");
    fill(&store, 3);
    overwrite(&store, 2, 4);
    for _ in 0..8 {
        store.compact_once().expect("compact");
    }
    let compaction = store.compaction_counters();
    assert!(compaction.segments_rewritten > 0, "no pass ran, so this proves nothing");
    assert!(compaction.tombstones_dropped > 0, "the tombstone was carried over a newer put");
    assert_eq!(value(&store), Some(b"second".to_vec()));
    assert!(walked(&store), "a walk lost the second put");
}

// a walk keeps a deleted key out once its grave is pruned, with its old version still sealed
#[test]
fn a_walk_keeps_a_deleted_key_out_once_its_grave_is_pruned() {
    let dir = TempDir::new().expect("temp dir");
    let store = ReelStore::open(dir.path().to_path_buf(), config(), COLUMNS).expect("open");
    Store::put(&store, "rows", &the_key(), b"first").expect("put");
    fill(&store, 1);
    Store::delete(&store, "rows", &the_key()).expect("delete");
    fill(&store, 2);
    store.index().prune_tombstones(Lsn(u64::MAX));
    assert_eq!(store.index().grave_count(), 0, "the grave stood, so this tested nothing");

    assert!(!walked(&store), "a key walk brought the deleted key back");
    let values = Store::iter_from(&store, "rows", &[], Direction::Asc)
        .expect("values")
        .any(|(key, _)| key == the_key());
    assert!(!values, "a value walk brought the deleted key back");
    assert_eq!(value(&store), None, "a get brought the deleted key back");
}
