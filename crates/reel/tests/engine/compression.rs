//! Compression at admission, observed end to end over the simulator
//!
//! A codec column's compressible payloads shrink on disk and read back whole through
//! every path a caller has, an incompressible payload stays raw, and a compaction
//! copy carries the codec byte rather than decoding anything. The volume's live-byte
//! accounting is the on-disk observation, since it counts stored bytes.

use std::path::PathBuf;
use std::sync::Arc;

use reel_core::Store;

use reel::io::fault::FaultPlan;
use reel::io::sim_backend::SimIo;
use reel::{
    ByteCount, Codec, ColumnId, ColumnSet, ColumnSpec, KeyWidth, MapShape, Preallocate, ReelConfig,
    ReelStore, SyncPolicy, ThreadBudget,
};

const RECORDS: u64 = 64;
const RECORD_LEN: usize = 4096;

const fn status(codec: Codec) -> ColumnSpec {
    ColumnSpec {
        id: ColumnId(1),
        name: "status",
        key_width: KeyWidth::Fixed(8),
        shard_bytes: 0,
        inline_max: 0,
        row_carry: 0,
        purge_mark: None,
        codec,
        map_shape: MapShape::Tree,
    }
}

const CODED: ColumnSet = &[status(Codec::Lz4)];
const RAW: ColumnSet = &[status(Codec::None)];

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

/// Protobuf-shaped bytes: repeated field tags, varint runs, and text
fn compressible(seed: u64, len: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(len);
    let mut at = seed;
    while out.len() < len {
        out.extend_from_slice(&[0x0a, 0x20]);
        out.extend_from_slice(&at.to_le_bytes());
        out.extend_from_slice(b"quantity=000000000;owner=11111111111111111111111111111111;");
        at = at.wrapping_add(1);
    }
    out.truncate(len);
    out
}

fn incompressible(seed: u64, len: usize) -> Vec<u8> {
    let mut state = seed | 1;
    let mut out = Vec::with_capacity(len);
    while out.len() < len {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        out.extend_from_slice(&state.to_le_bytes());
    }
    out.truncate(len);
    out
}

fn filled(columns: ColumnSet, payload: impl Fn(u64) -> Vec<u8>) -> (ReelStore, SimIo) {
    let sim = SimIo::new(FaultPlan::new(11));
    let store = ReelStore::open_with_io(
        PathBuf::from("/compression"),
        config(),
        columns,
        Arc::new(sim.clone()),
    )
    .expect("open");
    for at in 0..RECORDS {
        Store::put(&store, "status", &at.to_be_bytes(), &payload(at)).expect("put");
    }
    (store, sim)
}

// every read path returns the logical bytes of a compressed record
#[test]
fn coded_records_roundtrip_every_path() {
    let (store, _sim) = filled(CODED, |at| compressible(at, RECORD_LEN));

    for at in 0..RECORDS {
        let found = Store::get(&store, "status", &at.to_be_bytes())
            .expect("get")
            .expect("present");
        assert_eq!(
            &*found,
            &compressible(at, RECORD_LEN)[..],
            "point read at {at}"
        );
    }

    let mut walked = 0u64;
    for (key, found) in Store::iter(&store, "status").expect("iter") {
        let at = u64::from_be_bytes(key.as_slice().try_into().expect("key width"));
        assert_eq!(&*found, &compressible(at, RECORD_LEN)[..], "walk at {at}");
        walked += 1;
    }
    assert_eq!(walked, RECORDS);
}

// stored bytes shrink by at least the eighth admission demands
#[test]
fn coded_records_shrink_on_disk() {
    let (coded, _) = filled(CODED, |at| compressible(at, RECORD_LEN));
    let (raw, _) = filled(RAW, |at| compressible(at, RECORD_LEN));

    let coded_bytes = coded.totals().bytes.to_bytes();
    let raw_bytes = raw.totals().bytes.to_bytes();
    assert_eq!(raw_bytes, RECORDS * RECORD_LEN as u64);
    assert!(
        coded_bytes <= raw_bytes - raw_bytes / 8,
        "coded column stored {coded_bytes} of {raw_bytes} raw"
    );
}

// a payload the codec cannot shrink is stored raw and reads back unchanged
#[test]
fn incompressible_records_stay_raw() {
    let (store, _) = filled(CODED, |at| incompressible(at, RECORD_LEN));

    assert_eq!(store.totals().bytes.to_bytes(), RECORDS * RECORD_LEN as u64);
    for at in 0..RECORDS {
        let found = Store::get(&store, "status", &at.to_be_bytes())
            .expect("get")
            .expect("present");
        assert_eq!(&*found, &incompressible(at, RECORD_LEN)[..]);
    }
}

// a compaction copy relocates stored bytes and the codec byte together
#[test]
fn compaction_carries_the_codec_byte() {
    let sim = SimIo::new(FaultPlan::new(13));
    let store = ReelStore::open_with_io(
        PathBuf::from("/compaction"),
        ReelConfig {
            // segments small enough that the fill seals a few, and a threshold
            // low enough that the deletes below make them compactable
            segment_bytes: ByteCount::from_bytes(256 * 1024),
            alloc_chunk: ByteCount::from_bytes(64 * 1024),
            compact_dead_ratio: 0.3,
            ..config()
        },
        CODED,
        Arc::new(sim.clone()),
    )
    .expect("open");

    let keep = |at: u64| at.is_multiple_of(8);
    for at in 0..512u64 {
        Store::put(
            &store,
            "status",
            &at.to_be_bytes(),
            &compressible(at, RECORD_LEN),
        )
        .expect("put");
    }
    for at in 0..512u64 {
        if !keep(at) {
            Store::delete(&store, "status", &at.to_be_bytes()).expect("delete");
        }
    }

    for _ in 0..8 {
        store.compact_once().expect("compact");
    }

    for at in 0..512u64 {
        let found = Store::get(&store, "status", &at.to_be_bytes()).expect("get");
        match keep(at) {
            true => assert_eq!(
                &*found.expect("survivor present"),
                &compressible(at, RECORD_LEN)[..],
                "survivor at {at}"
            ),
            false => assert!(found.is_none(), "deleted key {at} came back"),
        }
    }
}
