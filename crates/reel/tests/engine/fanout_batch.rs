//! What a batch costs when each item also writes its own index rows
//!
//! Each item is written under its own wide identifier plus several secondary index
//! rows, all under one durability point, which is what any store keeping a secondary
//! index does on ingest. Two things make it its own shape: the keys are wide, 72 and
//! 108 bytes against payloads of a couple of hundred and one, so a record is mostly
//! key, and it fans out, so a batch of 64 items is 320 records across two columns.
//! The legs run each column alone, both together, the same records at narrow keys and
//! the same records in one column, which says whether the cost is the width, the
//! split or the fan-out.
//!
//! Ignored by default. Run with:
//!   cargo test -p tape-reel --test fanout_batch --release -- --ignored --nocapture
//! Knobs: FANOUT_ITEMS, FANOUT_WIDTH, FANOUT_BATCHES, FANOUT_VALUE, FANOUT_INDEX_VALUE

use std::time::{Duration, Instant};

use tempfile::TempDir;

use reel::{
    ByteCount, Codec, ColumnId, ColumnSet, ColumnSpec, KeyWidth, MapShape, RecordKey, RecordWrite,
    ReelConfig, ReelStore, SyncPolicy,
};

/// Bytes an item's own key takes: an identifier and the ordinal it landed under
const ITEM_KEY: usize = 72;

/// Bytes an index row's key takes: an address, the ordinal, a position, the identifier
const INDEX_KEY: usize = 108;

/// A narrow key, for the leg that asks what the width alone is worth
const NARROW_KEY: usize = 16;

const ITEM: ColumnId = ColumnId(1);
const INDEX: ColumnId = ColumnId(2);

fn items() -> usize {
    knob("FANOUT_ITEMS", 64)
}

/// Index rows each item writes beside itself
fn fanout() -> usize {
    knob("FANOUT_WIDTH", 4)
}

fn batches() -> usize {
    knob("FANOUT_BATCHES", 200)
}

fn item_value() -> usize {
    knob("FANOUT_VALUE", 176)
}

fn index_value() -> usize {
    knob("FANOUT_INDEX_VALUE", 1)
}

fn knob(name: &str, fallback: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|raw| raw.parse().ok())
        .unwrap_or(fallback)
}

/// Both columns at the widths the shape actually carries
fn wide_columns() -> ColumnSet {
    Box::leak(Box::new([
        column(ITEM, "item", ITEM_KEY),
        column(INDEX, "index", INDEX_KEY),
    ]))
}

/// The same two columns with the width taken out of them
fn narrow_columns() -> ColumnSet {
    Box::leak(Box::new([
        column(ITEM, "item", NARROW_KEY),
        column(INDEX, "index", NARROW_KEY),
    ]))
}

fn column(id: ColumnId, name: &'static str, key_width: usize) -> ColumnSpec {
    ColumnSpec {
        id,
        name,
        key_width: KeyWidth::Fixed(key_width as u16),
        shard_bytes: 0,
        inline_max: 0,
        row_carry: 0,
        purge_mark: None,
        codec: Codec::None,
        map_shape: MapShape::Tree,
    }
}

fn config() -> ReelConfig {
    ReelConfig {
        segment_bytes: ByteCount::mb(64),
        alloc_chunk: ByteCount::mb(16),
        sync: SyncPolicy::Never,
        ..ReelConfig::default()
    }
}

/// A key of the asked width, distinct per (batch, item, position)
///
/// The leading bytes move per record rather than per batch: a repeated prefix would
/// let the index share descents this shape does not get to share.
fn key(column: ColumnId, width: usize, batch: usize, item: usize, position: usize) -> RecordKey {
    let mut bytes = vec![0u8; width];
    let stamp = ((batch as u64) << 40) ^ ((item as u64) << 16) ^ position as u64;
    let mixed = stamp
        .wrapping_mul(0x9E37_79B9_7F4A_7C15)
        .rotate_left(29)
        .wrapping_mul(0xBF58_476D_1CE4_E5B9);
    bytes[..8].copy_from_slice(&mixed.to_be_bytes());
    if width >= 16 {
        bytes[8..16].copy_from_slice(&stamp.to_be_bytes());
    }
    RecordKey::from_bytes(column, &bytes).expect("key")
}

/// One batch of the shape, or the half of it a leg asked for
fn batch(shape: Shape, width: (usize, usize), at: usize) -> Vec<RecordWrite> {
    let mut writes = Vec::with_capacity(items() * (1 + fanout()));
    let item_payload = vec![0x2Eu8; item_value()];
    let index_payload = vec![0x3Fu8; index_value()];
    for item in 0..items() {
        if shape.writes_items() {
            writes.push(RecordWrite::Put {
                key: key(ITEM, width.0, at, item, 0),
                payload: item_payload.clone(),
            });
        }
        if shape.writes_index() {
            for position in 0..fanout() {
                let column = match shape {
                    Shape::OneColumn => ITEM,
                    Shape::ItemsOnly | Shape::IndexOnly | Shape::Both => INDEX,
                };
                writes.push(RecordWrite::Put {
                    key: key(column, width.1, at, item, position + 1),
                    payload: index_payload.clone(),
                });
            }
        }
    }
    writes
}

#[derive(Clone, Copy, PartialEq)]
enum Shape {
    ItemsOnly,
    IndexOnly,
    Both,
    OneColumn,
}

impl Shape {
    fn writes_items(self) -> bool {
        !matches!(self, Shape::IndexOnly)
    }

    fn writes_index(self) -> bool {
        !matches!(self, Shape::ItemsOnly)
    }
}

/// Drive one leg and report what a batch and a record cost in it
fn leg(name: &str, shape: Shape, columns: ColumnSet, width: (usize, usize)) {
    let dir = TempDir::new().expect("tempdir");
    let store = ReelStore::open(dir.path().to_path_buf(), config(), columns).expect("open");

    // One batch untimed, so segment creation is not charged to the first row.
    store.apply_batch(batch(shape, width, 0)).expect("warmup");

    let mut elapsed = Duration::ZERO;
    let mut records = 0u64;
    for at in 1..=batches() {
        let writes = batch(shape, width, at);
        records += writes.len() as u64;
        let began = Instant::now();
        store.apply_batch(writes).expect("batch");
        elapsed += began.elapsed();
    }
    store.flush().expect("flush");

    let per_batch = elapsed / batches() as u32;
    let per_record = elapsed / records.max(1) as u32;
    println!(
        "{name:<22} {:>7} {:>11} {:>13.2?} {:>13.2?}",
        records / batches() as u64,
        format!("{}/{}", width.0, width.1),
        per_batch,
        per_record,
    );
}

// what the fan-out costs, decomposed into width, split and count
#[test]
#[ignore = "measurement; run with --ignored --nocapture"]
fn fanout_batch_cost() {
    println!();
    println!(
        "one batch is {} items, each writing itself and {} index rows",
        items(),
        fanout(),
    );
    println!(
        "{:<22} {:>7} {:>11} {:>13} {:>13}",
        "leg", "records", "key widths", "per batch", "per record",
    );

    leg(
        "items only",
        Shape::ItemsOnly,
        wide_columns(),
        (ITEM_KEY, INDEX_KEY),
    );
    leg(
        "index only",
        Shape::IndexOnly,
        wide_columns(),
        (ITEM_KEY, INDEX_KEY),
    );
    leg(
        "both, the shape",
        Shape::Both,
        wide_columns(),
        (ITEM_KEY, INDEX_KEY),
    );
    leg(
        "both, narrow keys",
        Shape::Both,
        narrow_columns(),
        (NARROW_KEY, NARROW_KEY),
    );
    leg(
        "both, one column",
        Shape::OneColumn,
        wide_columns(),
        (ITEM_KEY, INDEX_KEY),
    );
}

// the shape writes every record it was asked to, which the cost legs assume
#[test]
fn every_record_of_the_shape_lands() {
    let dir = TempDir::new().expect("tempdir");
    let store = ReelStore::open(dir.path().to_path_buf(), config(), wide_columns()).expect("open");
    let writes = batch(Shape::Both, (ITEM_KEY, INDEX_KEY), 7);
    let asked = writes.len();
    store.apply_batch(writes).expect("batch");

    assert_eq!(
        asked,
        items() * (1 + fanout()),
        "the shape should be one row and its index rows per item",
    );
    for item in 0..items() {
        assert!(
            store
                .get(&key(ITEM, ITEM_KEY, 7, item, 0))
                .expect("get")
                .is_some(),
            "item {item} did not land",
        );
        for position in 0..fanout() {
            assert!(
                store
                    .get(&key(INDEX, INDEX_KEY, 7, item, position + 1))
                    .expect("get")
                    .is_some(),
                "index row {position} of item {item} did not land",
            );
        }
    }
}
