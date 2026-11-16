//! A store built, written, read, scanned and closed through the public surface alone

use std::path::PathBuf;

use reel::{Codec, ColumnConfig, DeviceClass, KeyShape, PlainIo, ReelStore, StoreConfig, SyncPolicy, ThreadCounts, VolumeConfig};
use reel_api::column_id::ColumnId;
use reel_api::direction::Direction;
use reel_api::scan::ScanStep;
use reel_api::units::{ByteCount, VolumeId};
use reel_api::write_batch::WriteBatch;

const COLUMN: ColumnId = ColumnId(1);
const BUDGET: ByteCount = ByteCount(128 * 1024 * 1024);
const KEYS: u32 = 100;

// a directory of its own under the system's temporary one
fn scratch_root() -> PathBuf {
    let root = std::env::temp_dir().join(format!("reel-public-api-{}", std::process::id()));
    let _gone = std::fs::remove_dir_all(&root);
    root
}

fn config(root: PathBuf) -> StoreConfig {
    StoreConfig {
        memory_budget: BUDGET,
        threads: ThreadCounts::default(),
        volumes: vec![VolumeConfig { name: "volume".to_string(), roots: vec![root], sync: SyncPolicy::Never, device: DeviceClass::Detect }],
        columns: vec![ColumnConfig {
            id: COLUMN,
            name: "column".to_string(),
            key_shape: KeyShape::Variable { most: 32 },
            codec: Codec::None,
            volume: VolumeId(0),
            complete_rows: None,
            sorted_rows: None,
            least_value: None,
        }],
    }
}

#[test]
fn a_store_opens_writes_reads_and_scans_from_outside() -> Result<(), Box<dyn std::error::Error>> {
    let root = scratch_root();
    let store = ReelStore::open(PlainIo::new(), &config(root.clone()))?;
    let mut batch = WriteBatch::new();
    for number in 0..KEYS {
        batch.put(COLUMN, format!("key-{number:03}").as_bytes(), format!("value-{number}").as_bytes());
    }
    store.write_batch(&batch)?;
    let mut value = Vec::new();
    assert!(store.get(COLUMN, b"key-007", &mut value)?);
    assert_eq!(value, b"value-7");
    let mut scan = store.iter_from(COLUMN, b"key-050", Direction::Ascending)?;
    let (mut key, mut found) = (Vec::new(), 0);
    while scan.next_row(&mut key, &mut value)? == ScanStep::Found {
        found += 1;
    }
    assert_eq!(found, KEYS - 50);
    drop(scan);
    store.close()?;
    std::fs::remove_dir_all(&root)?;
    Ok(())
}

// the rest of what a caller reaches: a volume from its constructor, deletes, a cue, a told scan, counts, the reports, and a
// read-only open of the closed store
#[test]
fn the_rest_of_the_surface_answers_from_outside() -> Result<(), Box<dyn std::error::Error>> {
    let root = scratch_root().with_extension("rest");
    let _gone = std::fs::remove_dir_all(&root);
    let mut config = config(root.clone());
    config.volumes = vec![VolumeConfig::new("volume", vec![root.clone()], SyncPolicy::Never)];
    let store = ReelStore::open(PlainIo::new(), &config)?;
    for number in 0..KEYS {
        store.put(COLUMN, format!("key-{number:03}").as_bytes(), b"value")?;
    }
    store.delete(COLUMN, b"key-000")?;
    store.delete_range(COLUMN, b"key-090", b"key-100")?;
    let cue = store.cue()?;
    let range = reel::from_key(COLUMN, b"key-010", Direction::Ascending).wanting(reel_api::units::RowCount(5));
    let mut scan = store.iter(&range)?;
    let (mut key, mut value, mut found) = (Vec::new(), Vec::new(), 0);
    while found < 5 && scan.next_row(&mut key, &mut value)? == ScanStep::Found {
        found += 1;
    }
    assert_eq!((found, key.as_slice()), (5, b"key-014".as_slice()));
    drop(scan);
    drop(cue);
    assert_eq!(store.count_prefix(COLUMN, b"key-0")?.0, 89);
    let mut report = reel_api::maintenance_report::MaintenanceReport::default();
    store.maintenance_report(&mut report);
    let _memory = store.memory_report();
    store.flush()?;
    store.close()?;
    drop(store);
    let reader = ReelStore::open_read_only(PlainIo::new(), &config)?;
    assert!(reader.get(COLUMN, b"key-050", &mut value)?);
    assert!(!reader.get(COLUMN, b"key-095", &mut value)?);
    reader.close()?;
    std::fs::remove_dir_all(&root)?;
    Ok(())
}
