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
    // a planner alone on its volume and one read thread
    config.threads = ThreadCounts { maintenance_workers: Some(0), read_threads: 1 };
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

// copy every file of a directory tree, so a store can be moved
fn copy_tree(from: &std::path::Path, to: &std::path::Path) -> std::io::Result<()> {
    std::fs::create_dir_all(to)?;
    for entry in std::fs::read_dir(from)? {
        let entry = entry?;
        let target = to.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_tree(&entry.path(), &target)?;
        } else if !entry.file_name().to_string_lossy().ends_with(".lock") {
            std::fs::copy(entry.path(), target)?;
        }
    }
    Ok(())
}

// a store on two roots, written and closed, under a root of its own
fn two_root_store(name: &str) -> Result<(PathBuf, StoreConfig), Box<dyn std::error::Error>> {
    let base = scratch_root().with_extension(name);
    let _gone = std::fs::remove_dir_all(&base);
    let mut config = config(base.join("first"));
    config.volumes[0].roots.push(base.join("second"));
    let store = ReelStore::open(PlainIo::new(), &config)?;
    for number in 0..KEYS {
        store.put(COLUMN, format!("key-{number:03}").as_bytes(), b"value")?;
    }
    store.close()?;
    Ok((base, config))
}

// a copy of a store's files opens at its new path, writable and read-only, and reads what the original held
#[test]
fn a_copied_store_opens_where_it_lies() -> Result<(), Box<dyn std::error::Error>> {
    let (base, config) = two_root_store("copied")?;
    let copy = base.with_extension("copy");
    let _gone = std::fs::remove_dir_all(&copy);
    copy_tree(&base, &copy)?;
    let mut moved = config.clone();
    moved.volumes[0].roots = vec![copy.join("first"), copy.join("second")];
    let mut value = Vec::new();
    let reader = ReelStore::open_read_only(PlainIo::new(), &moved)?;
    assert!(reader.get(COLUMN, b"key-042", &mut value)?);
    reader.close()?;
    drop(reader);
    let store = ReelStore::open(PlainIo::new(), &moved)?;
    assert!(store.get(COLUMN, b"key-042", &mut value)?);
    store.put(COLUMN, b"key-new", b"value")?;
    store.close()?;
    std::fs::remove_dir_all(&base)?;
    std::fs::remove_dir_all(&copy)?;
    Ok(())
}

// a config with fewer roots than the store has, or with a root of another store, is refused
#[test]
fn a_missing_root_is_refused() -> Result<(), Box<dyn std::error::Error>> {
    let (base, config) = two_root_store("missing")?;
    let mut fewer = config.clone();
    fewer.volumes[0].roots.truncate(1);
    let refused = ReelStore::open_read_only(PlainIo::new(), &fewer).err().map(|error| error.to_string());
    assert!(refused.as_deref().is_some_and(|message| message.contains("dropped a root")), "{refused:?}");
    let (other, other_config) = two_root_store("other")?;
    let mut mixed = config.clone();
    mixed.volumes[0].roots[1] = other_config.volumes[0].roots[1].clone();
    let refused = ReelStore::open_read_only(PlainIo::new(), &mixed).err().map(|error| error.to_string());
    assert!(refused.as_deref().is_some_and(|message| message.contains("different stores")), "{refused:?}");
    std::fs::remove_dir_all(&other)?;
    std::fs::remove_dir_all(&base)?;
    Ok(())
}

// the same roots listed in the other order open and read what the store held
#[test]
fn roots_in_another_order_read_right() -> Result<(), Box<dyn std::error::Error>> {
    let (base, config) = two_root_store("order")?;
    let mut swapped = config.clone();
    swapped.volumes[0].roots.reverse();
    let store = ReelStore::open(PlainIo::new(), &swapped)?;
    let mut value = Vec::new();
    for number in 0..KEYS {
        assert!(store.get(COLUMN, format!("key-{number:03}").as_bytes(), &mut value)?, "key {number}");
    }
    store.put(COLUMN, b"key-new", b"value")?;
    store.close()?;
    drop(store);
    let store = ReelStore::open(PlainIo::new(), &config)?;
    assert!(store.get(COLUMN, b"key-new", &mut value)?);
    assert!(store.get(COLUMN, b"key-042", &mut value)?);
    store.close()?;
    std::fs::remove_dir_all(&base)?;
    Ok(())
}
