//! The walk that lends its keys instead of handing them over
//!
//! An owned walk allocates and frees a key per row to move as little as eight bytes,
//! where the lending walk keeps the entry in the playback and takes the buffer back
//! on the next step. Reuse is the hazard: a buffer that served a wide key and then a
//! narrow one leaves the tail of the wide one behind unless it is cleared, and a page
//! or run boundary is where a buffer changes hands. So every case runs past both
//! boundaries and compares against the owned walk, which cannot carry that defect.

use tempfile::TempDir;

use reel_core::{Direction, Store};

use reel::{
    Codec, ColumnId, ColumnSet, ColumnSpec, KeyWidth, MapShape, ReelConfig, ReelStore, SyncPolicy,
};

/// Rows every walk steps
///
/// Well past the 32 row page floor and the 128 row run ceiling, so a walk refills its
/// page many times and settles at the widest run it will take.
const ROWS: u64 = 1000;

const COLUMNS: ColumnSet = &[
    ColumnSpec {
        id: ColumnId(1),
        name: "fixed",
        key_width: KeyWidth::Fixed(8),
        shard_bytes: 0,
        inline_max: 0,
        row_carry: 0,
        purge_mark: None,
        codec: Codec::None,
        map_shape: MapShape::Tree,
    },
    ColumnSpec {
        id: ColumnId(2),
        name: "wide",
        key_width: KeyWidth::Variable,
        shard_bytes: 0,
        inline_max: 0,
        row_carry: 0,
        purge_mark: None,
        codec: Codec::None,
        map_shape: MapShape::Tree,
    },
];

fn config() -> ReelConfig {
    ReelConfig {
        sync: SyncPolicy::Never,
        scrub_mbps: 0,
        ..ReelConfig::default()
    }
}

fn payload(row: u64) -> Vec<u8> {
    vec![(row % 251) as u8; 64 + (row % 33) as usize]
}

/// A key whose width swings row to row, so a reused buffer meets both directions
///
/// The widths cycle rather than climb, so a narrow key lands straight after a wide
/// one and back again inside one run.
fn wide_key(row: u64) -> Vec<u8> {
    let width = 4 + (row % 29) as usize;
    let mut bytes = row.to_be_bytes().to_vec();
    bytes.resize(8 + width, (row % 253) as u8);
    bytes
}

fn filled() -> (TempDir, ReelStore) {
    let dir = TempDir::new().expect("tempdir");
    let store = ReelStore::open(dir.path().to_path_buf(), config(), COLUMNS).expect("open");
    for row in 0..ROWS {
        Store::put(&store, "fixed", &row.to_be_bytes(), &payload(row)).expect("fixed put");
        Store::put(&store, "wide", &wide_key(row), &payload(row)).expect("wide put");
    }
    (dir, store)
}

/// Every pair the lending walk hands out, copied so the owned walk can be compared
fn lent(
    store: &ReelStore,
    cf: &str,
    start: Option<&[u8]>,
    way: Direction,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut walk = store.iter_lent(cf, start, way, 0).expect("lent walk");
    let mut out = Vec::new();
    while let Some((key, value)) = walk.next() {
        out.push((key.to_vec(), value.to_vec()));
    }
    out
}

fn owned(
    store: &ReelStore,
    cf: &str,
    start: Option<&[u8]>,
    way: Direction,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    let walk = match start {
        None => Store::iter(store, cf).expect("owned walk"),
        Some(start) => Store::iter_from(store, cf, start, way).expect("owned walk"),
    };
    walk.map(|(key, value)| (key, value.to_vec())).collect()
}

// the lending walk is the owned walk, key for key, across page and run refills
#[test]
fn a_lent_walk_matches_the_owned_one() {
    let (_dir, store) = filled();

    let lent = lent(&store, "fixed", None, Direction::Asc);
    assert_eq!(lent.len(), ROWS as usize, "the lending walk lost rows");
    assert_eq!(lent, owned(&store, "fixed", None, Direction::Asc));
}

// a narrow key after a wide one is the whole key, not the tail of the last one
#[test]
fn a_reused_buffer_does_not_widen_the_key_after_it() {
    let (_dir, store) = filled();

    let lent = lent(&store, "wide", None, Direction::Asc);
    assert_eq!(lent.len(), ROWS as usize, "the lending walk lost rows");
    assert_eq!(lent, owned(&store, "wide", None, Direction::Asc));

    // The comparison is worth nothing unless the widths really do move.
    let widths: std::collections::BTreeSet<usize> = lent.iter().map(|(key, _)| key.len()).collect();
    assert!(widths.len() > 1, "the wide column walked one key width");
}

// a bounded walk in either direction lends the same rows it would have handed over
#[test]
fn a_lent_walk_holds_its_bound_either_way() {
    let (_dir, store) = filled();
    let from = (ROWS / 2).to_be_bytes();

    for way in [Direction::Asc, Direction::Desc] {
        let lent = lent(&store, "fixed", Some(&from), way);
        assert_eq!(lent, owned(&store, "fixed", Some(&from), way), "{way:?}");
        assert!(!lent.is_empty(), "{way:?} lent nothing");
    }
}

// the step past the end is nothing, and stays nothing
#[test]
fn a_lent_walk_ends_once() {
    let (_dir, store) = filled();
    let mut walk = store
        .iter_lent("fixed", None, Direction::Asc, 0)
        .expect("lent walk");

    let mut rows = 0u64;
    while walk.next().is_some() {
        rows += 1;
    }
    assert_eq!(rows, ROWS);
    assert!(walk.next().is_none(), "the walk restarted past its end");
    assert!(walk.next().is_none(), "the walk restarted past its end");
}

// what the last step lent is gone, and what this one lends is the row after it
#[test]
fn a_step_invalidates_the_row_before_it() {
    let (_dir, store) = filled();
    let mut walk = store
        .iter_lent("fixed", None, Direction::Asc, 0)
        .expect("lent walk");

    let first = walk
        .next()
        .map(|(key, value)| (key.to_vec(), value.to_vec()));
    let (first_key, first_value) = first.expect("a first row");
    assert_eq!(first_key, 0u64.to_be_bytes(), "the walk started late");

    let (second_key, second_value) = walk.next().expect("a second row");
    assert_ne!(second_key, first_key.as_slice());
    assert_eq!(second_key, 1u64.to_be_bytes());
    assert_eq!(second_value, payload(1).as_slice());
    assert_ne!(second_value, first_value.as_slice());
}
