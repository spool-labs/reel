//! A store built, written, read, scanned and closed through the public surface alone

use std::path::PathBuf;

use reel::{Codec, ColumnConfig, ColumnIndex, DeviceClass, KeyShape, PlainIo, ReelStore, StoreConfig, SyncPolicy, ThreadCounts, VolumeConfig};
use reel_core::column_id::ColumnId;
use reel_core::direction::Direction;
use reel_core::scan::ScanStep;
use reel_core::units::{ByteCount, VolumeId};
use reel_core::write_batch::WriteBatch;

const COLUMN: ColumnId = ColumnId(1);
const BUDGET: ByteCount = ByteCount(128 * 1024 * 1024);
const KEYS: u32 = 100;

// a path under the system's temporary one, which each test gives its own extension and clears itself, so no test removes another's
fn scratch_root() -> PathBuf {
    std::env::temp_dir().join(format!("reel-public-api-{}", std::process::id()))
}

fn config(root: PathBuf) -> StoreConfig {
    StoreConfig {
        memory_budget: BUDGET,
        threads: ThreadCounts::default(),
        volumes: vec![VolumeConfig { name: "volume".to_string(), roots: vec![root], sync: SyncPolicy::Never, device: DeviceClass::Detect, open_segments: None, segment_bytes: None, lifetime_groups: None }],
        columns: vec![ColumnConfig {
            id: COLUMN,
            name: "column".to_string(),
            key_shape: KeyShape::Variable { most: 32 },
            codec: Codec::None,
            volume: VolumeId(0),
            index: ColumnIndex::Plain,
            least_rows: None,
            least_value: None,
        }],
    }
}

#[test]
fn a_store_opens_writes_reads_and_scans_from_outside() -> Result<(), Box<dyn std::error::Error>> {
    let root = scratch_root().with_extension("basic");
    let _gone = std::fs::remove_dir_all(&root);
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
    config.threads = ThreadCounts { maintenance_workers: Some(0), read_threads: 1, scrub_bytes_per_second: 0 };
    let store = ReelStore::open(PlainIo::new(), &config)?;
    for number in 0..KEYS {
        store.put(COLUMN, format!("key-{number:03}").as_bytes(), b"value")?;
    }
    store.delete(COLUMN, b"key-000")?;
    store.delete_range(COLUMN, b"key-090", b"key-100")?;
    let cue = store.cue()?;
    let range = reel::from_key(COLUMN, b"key-010", Direction::Ascending).wanting(reel_core::units::RowCount(5));
    let mut scan = store.iter(&range)?;
    let (mut key, mut value, mut found) = (Vec::new(), Vec::new(), 0);
    while found < 5 && scan.next_row(&mut key, &mut value)? == ScanStep::Found {
        found += 1;
    }
    assert_eq!((found, key.as_slice()), (5, b"key-014".as_slice()));
    drop(scan);
    drop(cue);
    assert_eq!(store.count_prefix(COLUMN, b"key-0")?.0, 89);
    let mut report = reel_core::maintenance_report::MaintenanceReport::default();
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

// a file's path, its length, the bytes the disk holds for it, and the bytes it holds past its end
type FileRow = (PathBuf, u64, u64, u64);


// every file under a directory, with its length, the bytes the disk holds for it, and the bytes it holds past its end
fn files_under(root: &std::path::Path, found: &mut Vec<FileRow>) -> std::io::Result<()> {
    use std::os::unix::fs::MetadataExt;
    for entry in std::fs::read_dir(root)? {
        let entry = entry?;
        let metadata = entry.metadata()?;
        if metadata.is_dir() {
            files_under(&entry.path(), found)?;
        } else {
            found.push((entry.path(), metadata.len(), metadata.blocks() * 512, held_past_end(&entry.path())?));
        }
    }
    Ok(())
}

// a finished store holds sealed segments alone, with no rows file and no room reserved past their ends, and a copy of it opens
// read-only and reads everything
#[test]
fn a_finished_store_holds_sealed_segments_alone() -> Result<(), Box<dyn std::error::Error>> {
    let root = scratch_root().with_extension("finished");
    let _gone = std::fs::remove_dir_all(&root);
    let config = config(root.clone());
    let store = ReelStore::open(PlainIo::new(), &config)?;
    for number in 0..KEYS {
        store.put(COLUMN, format!("key-{number:03}").as_bytes(), format!("value-{number}").as_bytes())?;
    }
    store.finish()?;
    drop(store);
    let mut files = Vec::new();
    files_under(&root, &mut files)?;
    for (path, length, held, past) in &files {
        println!("{} {length} B long, {held} B held, {past} B past its end", path.display());
    }
    assert!(files.iter().all(|(path, _length, _held, _past)| !path.to_string_lossy().ends_with(".rows")), "{files:?}");
    // no room past a file's end: the disk holds nothing past the file's length, rounded to its blocks
    assert!(files.iter().all(|(_path, _length, _held, past)| *past == 0), "{files:?}");
    let copy = root.with_extension("finished-copy");
    let _gone = std::fs::remove_dir_all(&copy);
    copy_tree(&root, &copy)?;
    let mut moved = config.clone();
    moved.volumes[0].roots = vec![copy.clone()];
    let reader = ReelStore::open_read_only(PlainIo::new(), &moved)?;
    let mut value = Vec::new();
    for number in 0..KEYS {
        assert!(reader.get(COLUMN, format!("key-{number:03}").as_bytes(), &mut value)?, "key {number}");
    }
    reader.close()?;
    std::fs::remove_dir_all(&root)?;
    std::fs::remove_dir_all(&copy)?;
    Ok(())
}

// a second open of a store another holds is refused with the holder, and opens once the first closes
#[test]
fn a_held_store_refuses_a_second_writer() -> Result<(), Box<dyn std::error::Error>> {
    let root = scratch_root().with_extension("held");
    let _gone = std::fs::remove_dir_all(&root);
    let config = config(root.clone());
    let first = ReelStore::open(PlainIo::new(), &config)?;
    let second = ReelStore::open(PlainIo::new(), &config);
    assert!(matches!(second, Err(reel_core::store_error::StoreError::Locked { .. })), "{:?}", second.err());
    first.close()?;
    drop(first);
    let second = ReelStore::open(PlainIo::new(), &config)?;
    second.close()?;
    std::fs::remove_dir_all(&root)?;
    Ok(())
}

// the random words a store's ids come from are never zero and never repeat
#[test]
fn random_words_differ() {
    use reel::Io;

    let io = PlainIo::new();
    let (first, second) = (io.random_word(), io.random_word());
    assert!(first != 0 && second != 0 && first != second, "{first} {second}");
}

// a sealed segment that reserved more than one step gives back what lies past its end
#[test]
fn a_sealed_segment_holds_no_room_past_its_end() -> Result<(), Box<dyn std::error::Error>> {
    let root = scratch_root().with_extension("sealed");
    let _gone = std::fs::remove_dir_all(&root);
    let config = config(root.clone());
    let store = ReelStore::open(PlainIo::new(), &config)?;
    let value = vec![7_u8; 64 * 1024];
    for number in 0..400_u32 {
        store.put(COLUMN, format!("key-{number:05}").as_bytes(), &value)?;
    }
    store.finish()?;
    drop(store);
    let mut files = Vec::new();
    files_under(&root, &mut files)?;
    let segments: Vec<_> = files.iter().filter(|(path, _length, _held, _past)| path.to_string_lossy().ends_with(".data")).collect();
    for (path, length, held, past) in &segments {
        println!("{} {length} B long, {held} B held, {past} B past its end", path.display());
    }
    assert!(segments.iter().any(|(_path, length, _held, _past)| *length > 8 * 1024 * 1024), "no segment passed one step: {segments:?}");
    assert!(segments.iter().all(|(_path, _length, _held, past)| *past == 0), "{segments:?}");
    std::fs::remove_dir_all(&root)?;
    Ok(())
}

// segments that seal because they filled give back the room reserved past their ends, as part-full ones do
#[test]
fn segments_sealed_by_filling_hold_no_room_past_their_ends() -> Result<(), Box<dyn std::error::Error>> {
    let root = scratch_root().with_extension("filled");
    let _gone = std::fs::remove_dir_all(&root);
    let config = config(root.clone());
    let store = ReelStore::open(PlainIo::new(), &config)?;
    let value = vec![9_u8; 64 * 1024];
    for number in 0..4_000_u32 {
        store.put(COLUMN, format!("key-{number:05}").as_bytes(), &value)?;
    }
    // close stops the jobs and leaves a filled segment whose seal hasn't finished for the next open, and finish is the call that
    // seals them all, so the test waits for a seal before it closes. A slow disk takes longer over one. The mix test below holds the
    // shape that kept blocks past a segment's end
    let sealed_files = |root: &std::path::Path| -> std::io::Result<(Vec<FileRow>, Vec<FileRow>)> {
        let mut files = Vec::new();
        files_under(root, &mut files)?;
        let rows: Vec<String> = files.iter().filter_map(|(path, _length, _held, _past)| path.to_string_lossy().strip_suffix(".rows").map(str::to_string)).collect();
        let sealed = files
            .iter()
            .filter(|(path, _length, _held, _past)| path.to_string_lossy().strip_suffix(".data").is_some_and(|stem| !rows.iter().any(|open| open == stem)))
            .cloned()
            .collect();
        Ok((files, sealed))
    };
    let waited = std::time::Instant::now();
    while sealed_files(&root)?.1.is_empty() && waited.elapsed() < std::time::Duration::from_secs(60) {
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    store.close()?;
    drop(store);
    let (files, sealed) = sealed_files(&root)?;
    for (path, length, held, past) in &sealed {
        println!("{} {length} B long, {held} B held, {past} B past its end", path.display());
    }
    assert!(!sealed.is_empty(), "no segment sealed in a minute: {files:?}");
    assert!(sealed.iter().all(|(_path, _length, _held, past)| *past == 0), "{sealed:?}");
    std::fs::remove_dir_all(&root)?;
    Ok(())
}

// a mix of value sizes, at the least budget, fills segments by bytes until one takes its reservation a step past the byte target:
// six columns, their key counts and value sizes
const MIX: [(u64, usize); 6] = [(95, 111_781), (95, 33_349), (13_009, 11_746), (39_995, 179), (157_183, 504), (604, 161_835)];

// segments that filled by bytes on a mix of value sizes give back the room past their ends, at the least budget
#[test]
fn segments_filled_by_a_mix_hold_no_room_past_their_ends() -> Result<(), Box<dyn std::error::Error>> {
    let root = scratch_root().with_extension("mix");
    let _gone = std::fs::remove_dir_all(&root);
    let mut config = config(root.clone());
    config.columns = (1..=MIX.len() as u8)
        .map(|id| ColumnConfig { id: ColumnId(id), name: format!("c{id}"), key_shape: KeyShape::Variable { most: 64 }, ..config.columns[0].clone() })
        .collect();
    config.memory_budget = ByteCount(1 << 20);
    let least = match ReelStore::open(PlainIo::new(), &config) {
        Err(reel_core::store_error::StoreError::BudgetTooSmall { least, .. }) => least.0,
        other => return Err(format!("a 1 MiB budget opened: {:?}", other.err()).into()),
    };
    config.memory_budget = ByteCount(least.div_ceil(1 << 20) << 20);
    let store = ReelStore::open(PlainIo::new(), &config)?;
    for (index, (keys, length)) in MIX.iter().enumerate() {
        let value = vec![index as u8; *length];
        for number in 0..*keys {
            store.put(ColumnId(index as u8 + 1), format!("c{index}/{number:08}").as_bytes(), &value)?;
        }
    }
    store.finish()?;
    drop(store);
    let mut files = Vec::new();
    files_under(&root, &mut files)?;
    let past: Vec<_> = files.iter().filter(|(path, _length, _held, past)| path.to_string_lossy().ends_with(".data") && *past > 0).collect();
    assert!(past.is_empty(), "segments holding blocks past their ends: {past:?}");
    std::fs::remove_dir_all(&root)?;
    Ok(())
}

// a store with no read thread answers the blocking calls, refuses the async ones by name, and holds no read slot or ring
#[test]
fn a_store_with_no_read_thread_answers_the_blocking_calls() -> Result<(), Box<dyn std::error::Error>> {
    use std::future::Future;
    use std::task::{Context, Poll, Waker};

    use reel_core::reads::Reads;
    use reel_core::store_error::StoreError;
    use reel_core::value_list::ValueList;

    let root = scratch_root().with_extension("blocking");
    let _gone = std::fs::remove_dir_all(&root);
    let mut config = config(root.clone());
    config.threads = ThreadCounts { maintenance_workers: Some(0), read_threads: 0, scrub_bytes_per_second: 0 };
    let store = ReelStore::open(PlainIo::new(), &config)?;
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
    // the refusal comes on the first poll, before any slot or worker is asked for
    let mut context = Context::from_waker(Waker::noop());
    let mut buf = Vec::new();
    let refused = std::pin::pin!(Reads::get_async(&store, COLUMN, b"key-007", &mut buf)).as_mut().poll(&mut context);
    assert!(matches!(refused, Poll::Ready(Err(StoreError::NoAsyncReads))), "{refused:?}");
    let mut keys = ValueList::new();
    let block = std::pin::pin!(scan.next_block(&mut keys)).as_mut().poll(&mut context);
    assert!(matches!(block, Poll::Ready(Err(StoreError::NoAsyncReads))), "{block:?}");
    drop(scan);
    let memory = store.memory_report();
    assert_eq!((memory.read_threads, memory.read_slots), (0, 0), "{memory:?}");
    store.close()?;
    std::fs::remove_dir_all(&root)?;
    Ok(())
}

// a volume of 256 KiB segments that wrote a few KB holds on disk within one reservation step of its data, and a set size past
// the format's limit is refused by name
#[test]
fn a_set_segment_size_keeps_a_small_store_small() -> Result<(), Box<dyn std::error::Error>> {
    use reel::{LEAST_SEGMENT_BYTES, MOST_SET_SEGMENT_BYTES};
    use reel_core::store_error::StoreError;

    let root = scratch_root().with_extension("small-segments");
    let _gone = std::fs::remove_dir_all(&root);
    let size = ByteCount(256 * 1024);
    // a set size reserves an eighth of itself a step
    let step = size.0 / 8;
    let mut small = config(root.clone());
    small.volumes[0].segment_bytes = Some(size);
    let store = ReelStore::open(PlainIo::new(), &small)?;
    for number in 0..64_u32 {
        store.put(COLUMN, format!("key-{number:03}").as_bytes(), &[3_u8; 64])?;
    }
    store.close()?;
    drop(store);
    let mut files = Vec::new();
    files_under(&root, &mut files)?;
    let data: Vec<_> = files.iter().filter(|(path, _length, _held, _past)| path.to_string_lossy().ends_with(".data")).collect();
    let held: u64 = data.iter().map(|(_path, _length, held, _past)| *held).sum();
    println!("{} data files, {held} B held, a step of {step} B", data.len());
    // the rows took one record window, which a segment counts whole once a writer lands in it, and the room a step past it
    assert!(held <= WINDOW_BYTES + step, "{held} B held: {data:?}");
    std::fs::remove_dir_all(&root)?;

    for asked in [ByteCount(LEAST_SEGMENT_BYTES.0 - 1), ByteCount(MOST_SET_SEGMENT_BYTES.0 + 1)] {
        let mut config = config(scratch_root().with_extension("refused-segments"));
        config.volumes[0].segment_bytes = Some(asked);
        let refused = ReelStore::open(PlainIo::new(), &config).map(|_store| ());
        assert!(
            matches!(refused, Err(StoreError::SegmentSize { asked: refused_size, least, most }) if refused_size == asked && least == LEAST_SEGMENT_BYTES && most == MOST_SET_SEGMENT_BYTES),
            "{refused:?}"
        );
    }
    Ok(())
}

// the file range a slot's small records fill before one write call, which a segment takes whole
const WINDOW_BYTES: u64 = 64 * 1024;

// a column left to the default keeps a sorted index on fixed-width keys and none on variable ones, a fixed-width column can opt
// out, and a sorted index on variable-width keys is refused with the reason
#[test]
fn a_column_index_follows_its_keys_by_default() -> Result<(), Box<dyn std::error::Error>> {
    use reel::ColumnIndex;
    use reel_core::maintenance_report::MaintenanceReport;
    use reel_core::store_error::StoreError;

    let root = scratch_root().with_extension("kinds");
    let _gone = std::fs::remove_dir_all(&root);
    // the constructor leaves the index to the keys, and a config names one only to choose otherwise
    let column = |id: u8, key_shape: KeyShape| ColumnConfig::new(ColumnId(id), format!("column-{id}"), key_shape);
    let fixed = KeyShape::Fixed { width: 8 };
    let mut kinds = config(root.clone());
    kinds.columns = vec![
        column(1, fixed),
        column(2, KeyShape::Variable { most: 32 }),
        ColumnConfig { index: ColumnIndex::Plain, ..column(3, fixed) },
    ];
    let store = ReelStore::open(PlainIo::new(), &kinds)?;
    let mut report = MaintenanceReport::new();
    store.maintenance_report(&mut report);
    let sorted: Vec<u8> = report.sorted_columns.iter().map(|reading| reading.column.0).collect();
    assert_eq!(sorted, vec![1], "{:?}", report.sorted_columns);
    store.close()?;
    std::fs::remove_dir_all(&root)?;

    let mut refused = config(scratch_root().with_extension("kinds-refused"));
    refused.columns = vec![ColumnConfig { index: ColumnIndex::Sorted, ..column(1, KeyShape::Variable { most: 32 }) }];
    let outcome = ReelStore::open(PlainIo::new(), &refused).map(|_store| ());
    assert!(matches!(&outcome, Err(StoreError::BadIndex { reason, .. }) if reason.contains("fixed width")), "{outcome:?}");
    Ok(())
}

// the least budget a call gives with no store opened is the one a store opens at and a byte under it is refused, and the one a
// refusal for its budget gives, for a writer and for a reader, over six shapes
#[test]
fn the_least_budget_needs_no_trial_open() -> Result<(), Box<dyn std::error::Error>> {
    use reel_core::store_error::StoreError;

    let tiny = ByteCount(1024 * 1024);
    let base = scratch_root().with_extension("least");
    let _gone = std::fs::remove_dir_all(&base);
    let shapes: Vec<(&str, Shape)> = vec![
        ("default", Box::new(config)),
        ("small", Box::new(|root| {
            let mut small = config(root);
            small.threads = ThreadCounts { maintenance_workers: Some(0), read_threads: 0, scrub_bytes_per_second: 0 };
            small.volumes[0].open_segments = Some(1);
            small.volumes[0].segment_bytes = Some(ByteCount(256 * 1024));
            small.volumes[0].lifetime_groups = Some(0);
            small
        })),
        ("three columns, one lz4", Box::new(|root| {
            let mut three = config(root);
            three.columns.push(ColumnConfig { codec: Codec::Lz4, ..ColumnConfig::new(ColumnId(2), "bulk", KeyShape::Variable { most: 32 }) });
            three.columns.push(ColumnConfig::new(ColumnId(3), "plain", KeyShape::Variable { most: 32 }));
            three
        })),
        ("sorted", Box::new(|root| {
            let mut sorted = config(root);
            sorted.columns = vec![ColumnConfig::new(ColumnId(1), "column", KeyShape::Fixed { width: 16 })];
            sorted
        })),
        ("two volumes", Box::new(|root: PathBuf| {
            let mut two = config(root.join("first"));
            two.volumes.push(VolumeConfig::new("second", vec![root.join("second")], SyncPolicy::Never));
            two.columns.push(ColumnConfig { volume: VolumeId(1), ..ColumnConfig::new(ColumnId(2), "other", KeyShape::Variable { most: 32 }) });
            two
        })),
        ("two workers", Box::new(|root| {
            let mut workers = config(root);
            workers.threads = ThreadCounts { maintenance_workers: Some(2), ..ThreadCounts::default() };
            workers
        })),
    ];
    for (name, shape) in &shapes {
        let root = base.join(name.replace(' ', "-"));
        let at = |budget: ByteCount| StoreConfig { memory_budget: budget, ..shape(root.clone()) };
        let writer = ReelStore::least_budget(&PlainIo::new(), &at(tiny))?;
        // a store opens at the least the call gives and is refused a byte under it, and a refusal for its budget gives the same
        assert!(ReelStore::open(PlainIo::new(), &at(ByteCount(writer.0 - 1))).is_err(), "{name}: opened under its least");
        if let Err(StoreError::BudgetTooSmall { least, .. }) = ReelStore::open(PlainIo::new(), &at(tiny)) {
            assert_eq!(writer, least, "{name}: the writer's least");
        }
        let store = ReelStore::open(PlainIo::new(), &at(writer))?;
        store.put(ColumnId(1), &[1; 16], b"value")?;
        store.finish()?;
        drop(store);
        let reader = ReelStore::least_read_only_budget(&PlainIo::new(), &at(tiny))?;
        assert!(ReelStore::open_read_only(PlainIo::new(), &at(ByteCount(reader.0 - 1))).is_err(), "{name}: a view opened under its least");
        drop(ReelStore::open_read_only(PlainIo::new(), &at(reader))?);
        if let Err(StoreError::BudgetTooSmall { least, .. }) = ReelStore::open_read_only(PlainIo::new(), &at(tiny)) {
            assert_eq!(reader, least, "{name}: the reader's least");
        }
        println!("{name}: writer {} B, reader {} B", writer.0, reader.0);
        assert!(reader <= writer, "{name}: a reader takes more than a writer");
    }
    std::fs::remove_dir_all(&base)?;
    Ok(())
}

// a config built on a root
type Shape = Box<dyn Fn(PathBuf) -> StoreConfig>;

// a finished store of a few KB on 256 KiB segments holds its rows in one small record window and the footer, with nothing reserved
#[test]
fn a_finished_small_store_holds_little_past_its_rows() -> Result<(), Box<dyn std::error::Error>> {
    let root = scratch_root().with_extension("finished-small");
    let _gone = std::fs::remove_dir_all(&root);
    let mut small = config(root.clone());
    small.volumes[0].segment_bytes = Some(ByteCount(256 * 1024));
    let store = ReelStore::open(PlainIo::new(), &small)?;
    for number in 0..64_u32 {
        store.put(COLUMN, format!("key-{number:03}").as_bytes(), &[3_u8; 64])?;
    }
    store.finish()?;
    drop(store);
    let mut files = Vec::new();
    files_under(&root, &mut files)?;
    let data: Vec<_> = files.iter().filter(|(path, _length, _held, _past)| path.to_string_lossy().ends_with(".data")).collect();
    let length: u64 = data.iter().map(|(_path, length, _held, _past)| *length).sum();
    let held: u64 = data.iter().map(|(_path, _length, held, _past)| *held).sum();
    println!("{} data files, {length} B long, {held} B held, for 64 rows of 64 B", data.len());
    // one data file of the rows in an 8 KiB window and the footer's fixed parts, which a segment of any size carries
    assert!(data.len() == 1 && length <= 176 * 1024 && held <= length.next_multiple_of(4_096), "{data:?}");
    std::fs::remove_dir_all(&root)?;
    Ok(())
}

// the bytes a file holds on disk past its length rounded to a block. Linux asks the extents, since ext4 counts the block of an extent
// tree among a file's blocks once the file has more than four extents, and that block holds no room past the end
#[cfg(target_os = "linux")]
fn held_past_end(path: &std::path::Path) -> std::io::Result<u64> {
    use std::os::fd::AsRawFd;
    const FS_IOC_FIEMAP: libc::c_ulong = 0xC020_660B;
    const FIEMAP_FLAG_SYNC: u32 = 1;
    const FIEMAP_EXTENT_LAST: u32 = 1;
    const EXTENTS: usize = 64;
    #[repr(C)]
    #[derive(Clone, Copy, Default)]
    struct Extent {
        logical: u64,
        physical: u64,
        length: u64,
        reserved64: [u64; 2],
        flags: u32,
        reserved: [u32; 3],
    }
    #[repr(C)]
    struct Map {
        start: u64,
        length: u64,
        flags: u32,
        mapped: u32,
        count: u32,
        reserved: u32,
        extents: [Extent; EXTENTS],
    }
    let file = std::fs::File::open(path)?;
    let end = file.metadata()?.len().next_multiple_of(4_096);
    let mut past = 0;
    let mut start = end;
    loop {
        let mut map = Map { start, length: u64::MAX - start, flags: FIEMAP_FLAG_SYNC, mapped: 0, count: EXTENTS as u32, reserved: 0, extents: [Extent::default(); EXTENTS] };
        // SAFETY: the map is a fiemap header with room for `count` extents, which the kernel fills and never passes
        let asked = unsafe { libc::ioctl(file.as_raw_fd(), FS_IOC_FIEMAP, &mut map) };
        if asked != 0 {
            let error = std::io::Error::last_os_error();
            if error.raw_os_error() != Some(libc::EOPNOTSUPP) {
                return Err(error);
            }
            // a filesystem with no extent map, as tmpfs, keeps no extent tree, so its blocks are the file's own
            use std::os::unix::fs::MetadataExt;
            return Ok((file.metadata()?.blocks() * 512).saturating_sub(end));
        }
        let extents = &map.extents[..map.mapped as usize];
        for extent in extents {
            let from = extent.logical.max(end);
            past += (extent.logical + extent.length).saturating_sub(from);
        }
        match extents.last() {
            Some(last) if last.flags & FIEMAP_EXTENT_LAST == 0 => start = last.logical + last.length,
            _ => return Ok(past),
        }
    }
}

// the bytes a file holds on disk past its length rounded to a block
#[cfg(not(target_os = "linux"))]
fn held_past_end(path: &std::path::Path) -> std::io::Result<u64> {
    use std::os::unix::fs::MetadataExt;
    let metadata = std::fs::metadata(path)?;
    Ok((metadata.blocks() * 512).saturating_sub(metadata.len().next_multiple_of(4_096)))
}
