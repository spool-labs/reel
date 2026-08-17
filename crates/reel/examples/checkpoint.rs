//! A checkpoint is the volume at a cue, and it opens as a volume
//!
//! A cue holds a consistent view open in process; a checkpoint puts that same view
//! on disk in a sibling directory, as one hard link per sealed segment, so the cost
//! is metadata rather than bytes. The copy then opens as a store of its own while
//! the original keeps taking writes that never reach it.
//!
//! cargo run --example checkpoint

use std::ops::Range;
use std::path::Path;

use tempfile::TempDir;

use reel::format::column::RecordKey;
use reel::{
    ByteCount, Codec, ColumnId, ColumnSet, ColumnSpec, IndexResidency, KeyWidth, MapShape,
    ReelConfig, ReelStore, SyncPolicy, ThreadBudget,
};
use reel_core::Value;

const RECORDS: ColumnId = ColumnId(1);

const COLUMNS: ColumnSet = &[ColumnSpec {
    id: RECORDS,
    name: "records",
    key_width: KeyWidth::Fixed(KEY_LEN as u16),
    shard_bytes: 2,
    inline_max: 0,
    row_carry: 0,
    purge_mark: None,
    codec: Codec::None,
    map_shape: MapShape::Tree,
}];

const KEY_LEN: usize = 16;
const PAYLOAD_LEN: usize = 4_096;

/// Records written before the cue, which is everything the copy owes back
const COPIED: u32 = 300;

/// Records written after it, which the copy must not have
const LATER: u32 = 100;

const EARLY_FILL: u8 = 0xa1;
const LATE_FILL: u8 = 0xc3;
const OVERWRITE_FILL: u8 = 0xff;

/// Small segments, so a modest write count rolls several and the link set is real
fn config() -> ReelConfig {
    ReelConfig {
        segment_bytes: ByteCount::mb(1),
        alloc_chunk: ByteCount::from_bytes(64 * 1024),
        sync: SyncPolicy::Never,
        active_tails: ThreadBudget::threads(2),
        index: IndexResidency::Resident,
        ..ReelConfig::default()
    }
}

fn key(at: u32) -> RecordKey {
    let mut bytes = [0u8; KEY_LEN];
    bytes[..4].copy_from_slice(&at.to_be_bytes());
    RecordKey::from_bytes(RECORDS, &bytes).expect("key")
}

fn open(dir: &Path) -> reel::Result<ReelStore> {
    ReelStore::open(dir.to_path_buf(), config(), COLUMNS)
}

fn fill(store: &ReelStore, range: Range<u32>, byte: u8) -> reel::Result<()> {
    for at in range {
        store.put(&key(at), &vec![byte; PAYLOAD_LEN])?;
    }
    store.flush()
}

fn main() -> reel::Result<()> {
    let home = TempDir::new().expect("tempdir");
    let live = home.path().join("live");
    let copy = home.path().join("copy");

    let store = open(&live)?;
    fill(&store, 0..COPIED, EARLY_FILL)?;

    let cue = store.cue()?;
    let taken = store.checkpoint(&copy)?;
    assert!(taken.segments > 0, "a filled volume linked no segments");
    println!(
        "checkpoint at sequence {} linked {} segments",
        taken.at.as_u64(),
        taken.segments,
    );

    // The original moves on: new keys, and a new version of one the copy holds.
    fill(&store, COPIED..COPIED + LATER, LATE_FILL)?;
    store.put(&key(0), &vec![OVERWRITE_FILL; PAYLOAD_LEN])?;
    store.flush()?;

    assert_eq!(
        store.get(&key(0))?.map(Value::into_vec),
        Some(vec![OVERWRITE_FILL; PAYLOAD_LEN]),
    );
    assert_eq!(
        store.get_at(&key(0), &cue)?.map(Value::into_vec),
        Some(vec![EARLY_FILL; PAYLOAD_LEN]),
        "the held cue saw the overwrite it was taken before",
    );
    assert!(
        taken.at < store.sequence(),
        "the cue did not sit below what came after"
    );
    drop(cue);

    let restored = open(&copy)?;
    for at in 0..COPIED {
        assert_eq!(
            restored.get(&key(at))?.map(Value::into_vec),
            Some(vec![EARLY_FILL; PAYLOAD_LEN]),
            "record {at} did not survive the checkpoint",
        );
    }
    for at in COPIED..COPIED + LATER {
        assert_eq!(
            restored.get(&key(at))?,
            None,
            "record {at} was written after the cue and is in the copy",
        );
    }
    assert_eq!(restored.totals().count, u64::from(COPIED));

    println!(
        "the live volume is at sequence {}",
        store.sequence().as_u64()
    );
    println!(
        "the copy opened on its own and serves {} records, none of the {LATER} written since",
        restored.totals().count,
    );

    Ok(())
}
