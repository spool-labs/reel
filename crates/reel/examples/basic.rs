//! Open a reel on a temporary directory and work one caller-declared column
//!
//! Puts, a point read, a delete, a batched read, and a close that seals the open
//! segment, through both doors onto a record: the store trait, which resolves a
//! column family name per call, and the inherent method, which takes a key the
//! caller resolved once.
//!
//! cargo run --example basic

use tempfile::TempDir;

use reel::{
    ByteCount, Codec, ColumnId, ColumnSet, ColumnSpec, KeyWidth, MapShape, Preallocate, RecordKey,
    ReelConfig, ReelStore, Store, StoreResult, ThreadBudget,
};

/// Family name the trait addresses, and the identifier its records carry
const RECORDS: &str = "records";
const RECORDS_COLUMN: ColumnId = ColumnId(1);

/// Eight byte keys, one index shard, values stored raw
const COLUMNS: ColumnSet = &[ColumnSpec {
    id: RECORDS_COLUMN,
    name: RECORDS,
    key_width: KeyWidth::Fixed(8),
    shard_bytes: 0,
    inline_max: 0,
    row_carry: 0,
    purge_mark: None,
    codec: Codec::None,
    map_shape: MapShape::Tree,
}];

/// Small enough that the run writes a file rather than a gibibyte of zeros
const SEGMENT_BYTES: ByteCount = ByteCount::mb(4);
const ALLOC_CHUNK: ByteCount = ByteCount::mb(1);

/// Records written, and which one of them is deleted again
const RECORD_COUNT: u64 = 4;
const DELETED: u64 = 1;

fn key_of(number: u64) -> [u8; 8] {
    number.to_be_bytes()
}

fn payload_of(number: u64) -> Vec<u8> {
    format!("payload {number}").into_bytes()
}

fn main() -> StoreResult<()> {
    let root = TempDir::new()?;
    let config = ReelConfig {
        segment_bytes: SEGMENT_BYTES,
        alloc_chunk: ALLOC_CHUNK,
        preallocate: Preallocate::Chunk,
        active_tails: ThreadBudget::threads(1),
        ..ReelConfig::default()
    };
    let store = ReelStore::open(root.path().to_path_buf(), config, COLUMNS)?;

    for number in 0..RECORD_COUNT {
        Store::put(&store, RECORDS, &key_of(number), &payload_of(number))?;
    }
    println!("wrote {RECORD_COUNT} records to {RECORDS}");

    let by_name = Store::get(&store, RECORDS, &key_of(0))?.expect("record 0");
    assert_eq!(&*by_name, payload_of(0).as_slice());

    // A RecordKey is the name resolution held, so the inherent call skips the
    // lookup the trait call repeats per read.
    let resolved = RecordKey::from_bytes(RECORDS_COLUMN, &key_of(0))?;
    let by_key = store.get(&resolved)?.expect("record 0 by resolved key");
    assert_eq!(&*by_key, &*by_name);
    println!("Store::get and ReelStore::get agree on record 0");

    Store::delete(&store, RECORDS, &key_of(DELETED))?;
    assert!(!Store::contains(&store, RECORDS, &key_of(DELETED))?);

    let mut keys = Vec::with_capacity(RECORD_COUNT as usize);
    for number in 0..RECORD_COUNT {
        keys.push(key_of(number));
    }
    let mut asked: Vec<&[u8]> = Vec::with_capacity(keys.len());
    for key in &keys {
        asked.push(key.as_slice());
    }

    let answers = Store::get_many(&store, RECORDS, &asked)?;
    assert_eq!(answers.len(), RECORD_COUNT as usize);
    assert!(answers[DELETED as usize].is_none());

    let mut present = 0u64;
    for (number, answer) in answers.iter().enumerate() {
        let Some(value) = answer else {
            continue;
        };
        assert_eq!(&**value, payload_of(number as u64).as_slice());
        present += 1;
    }
    assert_eq!(present, RECORD_COUNT - 1);
    println!("deleted record {DELETED}, get_many answered {present} of {RECORD_COUNT}");

    let totals = store.totals();
    assert_eq!(totals.count, RECORD_COUNT - 1);

    store.close()?;
    println!(
        "closed holding {} live records over {} payload bytes",
        totals.count,
        totals.bytes.to_bytes(),
    );

    Ok(())
}
