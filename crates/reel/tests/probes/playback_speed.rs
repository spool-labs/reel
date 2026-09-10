//! What a playback costs a paged column as its sealed segments multiply
//!
//! A merge answers two questions per key: which open cursor holds the next one, and
//! which others hold the same key. Read the per-key column: a scan over every cursor is
//! linear in them, so a per-key cost rising with the segment count is the scan showing
//! up, and one that flattens is the ordering no longer being what is paid for.
//!
//! Opt-in. Run with:
//!   cargo test -p tape-reel --release --test probes -- playback_speed

use std::ops::Bound;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use reel::io::fault::FaultPlan;
use reel::io::sim_backend::SimIo;
use reel::{
    ByteCount, Codec, ColumnId, ColumnSet, ColumnSpec, IndexResidency, KeyPage, KeyWidth, MapShape,
    PlaybackCursor, Preallocate, RecordKey, ReelConfig, ReelStore, SyncPolicy, ThreadBudget, Way,
    MAP_EVERYTHING,
};

/// Virtual root the simulator's files live under
const ROOT: &str = "/bulk";

const RECORDS: ColumnId = ColumnId(1);
const BLOB: ColumnId = ColumnId(2);

/// Bytes a record key occupies: two group bytes then a thirty-two byte id
const RECORD_KEY_LEN: usize = 34;

/// The columns the volume is opened with
const COLUMNS: ColumnSet = &[
    ColumnSpec {
        id: RECORDS,
        name: "records",
        key_width: KeyWidth::Fixed(RECORD_KEY_LEN as u16),
        shard_bytes: 2,
        inline_max: 0,
        row_carry: 0,
        purge_mark: None,
        codec: Codec::None,
        map_shape: MapShape::Tree,
    },
    ColumnSpec {
        id: BLOB,
        name: "blob_data",
        key_width: KeyWidth::Fixed(32),
        shard_bytes: 0,
        inline_max: 0,
        row_carry: 0,
        purge_mark: None,
        codec: Codec::None,
        map_shape: MapShape::Tree,
    },
];

/// The group every record here is written under
const GROUP: u16 = 7;

/// Payload every record carries, small so the segment count comes from keys
const PAYLOAD: usize = 64;

/// Segment size, which sets how many keys each one holds
const SEGMENT: u64 = 64 * 1024;

/// Keys a page of the playback asks for, the store's own first page size
const PAGE: usize = 256;

/// Sealed segment counts to playback over
const CASES: &[usize] = &[1, 4, 16, 64, 256];

fn config() -> ReelConfig {
    ReelConfig {
        segment_bytes: ByteCount::from_bytes(SEGMENT),
        alloc_chunk: ByteCount::from_bytes(8 * 1024),
        preallocate: Preallocate::Chunk,
        sync: SyncPolicy::Never,
        active_tails: ThreadBudget::threads(1),
        index: IndexResidency::Paged,
        // The read path the engine serves callers with
        map_above: MAP_EVERYTHING,
        ..ReelConfig::default()
    }
}

/// The record column key for a group and an id, big endian group at the front
fn record_key(group: u16, id: [u8; 32]) -> RecordKey {
    let mut bytes = [0u8; RECORD_KEY_LEN];
    bytes[..2].copy_from_slice(&group.to_be_bytes());
    bytes[2..].copy_from_slice(&id);
    RecordKey::from_bytes(RECORDS, &bytes).expect("key")
}

fn id(at: u64) -> [u8; 32] {
    let mut bytes = [0u8; 32];
    bytes[..8].copy_from_slice(&at.to_be_bytes());
    bytes
}

// how a paged playback scales with the segments it has to merge
pub fn walk_by_segment_count() {
    // libtest leaves the test name line open, so a header needs a newline ahead of it.
    println!();
    println!(
        "{:>9}  {:>9}  {:>12}  {:>12}",
        "segments", "keys", "playback", "per key"
    );

    for &wanted in CASES {
        let sim = SimIo::new(FaultPlan::new(1));
        let store = ReelStore::open_with_io(
            PathBuf::from(ROOT),
            config(),
            COLUMNS,
            Arc::new(sim.clone()),
        )
        .expect("open");

        let payload = vec![0xa5u8; PAYLOAD];
        let mut written = 0u64;
        while store.index().segments_snapshot().len() < wanted + 1 {
            store
                .put(&record_key(GROUP, id(written)), &payload)
                .expect("put");
            written += 1;
        }
        store.flush().expect("flush");
        // Hand every sealed segment's keys to its footer, so the playback below is a
        // merge over them rather than a read of the map.
        let paged = store.page_out_sealed().expect("page out");
        assert!(paged > 0, "a paged playback needs keys in footers");

        let mut playback =
            PlaybackCursor::new(RECORDS, Way::Up, Bound::Unbounded).expect("playback");
        let mut page = KeyPage::with_lens();
        let mut seen = 0u64;
        let start = Instant::now();
        while !playback.is_done() {
            store
                .page_from(&mut playback, PAGE, &mut page)
                .expect("page");
            seen += page.len() as u64;
        }
        let elapsed = start.elapsed();

        assert_eq!(seen, written, "the playback saw every key it wrote");
        println!(
            "{:>9}  {:>9}  {:>12.2?}  {:>12.2?}",
            store.index().segments_snapshot().len(),
            seen,
            elapsed,
            elapsed / seen as u32,
        );
    }
}
