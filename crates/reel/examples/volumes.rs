//! One reel spread over three roots, and what survives losing one of them
//!
//! Two fast roots take the writes and a capacity root takes nothing fresh. The
//! manifest on the home root names the set and each extra root carries a marker
//! proving it belongs. Once a root is gone an open that has not been told about
//! it refuses; declared dead, the same open serves the survivors, answers the
//! lost records as plain misses, and keeps taking writes.
//!
//! cargo run --example volumes

use std::path::{Path, PathBuf};

use tempfile::TempDir;

use reel::{
    ByteCount, Codec, ColumnId, ColumnSet, ColumnSpec, KeyWidth, MapShape, Preallocate, RecordKey,
    ReelConfig, ReelStore, SyncPolicy, ThreadBudget, VolumeSpec,
};

const RECORDS: ColumnId = ColumnId(1);

/// Small segments against an eight kibibyte payload, so a short run seals several
const SEGMENT_BYTES: ByteCount = ByteCount::from_bytes(256 * 1024);
const ALLOC_CHUNK: ByteCount = ByteCount::from_bytes(64 * 1024);
const RECORD_COUNT: u64 = 120;
const PAYLOAD_BYTES: usize = 8 * 1024;

const COLUMNS: ColumnSet = &[ColumnSpec {
    id: RECORDS,
    name: "records",
    key_width: KeyWidth::Fixed(16),
    shard_bytes: 0,
    inline_max: 0,
    row_carry: 0,
    purge_mark: None,
    codec: Codec::None,
    map_shape: MapShape::Tree,
}];

fn key(at: u64) -> RecordKey {
    let mut bytes = [0u8; 16];
    bytes[8..].copy_from_slice(&at.to_be_bytes());
    RecordKey::from_bytes(RECORDS, &bytes).expect("key")
}

/// The extra roots as config states them: home is the root `open` is given
fn config(warm: &Path, cold: &Path) -> ReelConfig {
    ReelConfig {
        segment_bytes: SEGMENT_BYTES,
        alloc_chunk: ALLOC_CHUNK,
        preallocate: Preallocate::Chunk,
        sync: SyncPolicy::Never,
        active_tails: ThreadBudget::threads(1),
        volumes: vec![VolumeSpec::fast(warm), VolumeSpec::capacity(cold)],
        ..ReelConfig::default()
    }
}

/// The segment files a root holds, smallest id first
fn segments_in(root: &Path) -> Vec<PathBuf> {
    let mut found: Vec<PathBuf> = std::fs::read_dir(root)
        .expect("read root")
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| {
            path.extension()
                .is_some_and(|extension| extension == "reel")
        })
        .collect();
    found.sort();
    found
}

fn main() {
    let home = TempDir::new().expect("home");
    let holder = TempDir::new().expect("holder");
    let cold = TempDir::new().expect("cold");
    let warm = holder.path().join("warm");
    std::fs::create_dir(&warm).expect("warm root");
    let roots = config(&warm, cold.path());

    let store = ReelStore::open(home.path().to_path_buf(), roots.clone(), COLUMNS).expect("open");
    let payload = vec![0x5Au8; PAYLOAD_BYTES];
    for at in 0..RECORD_COUNT {
        store.put(&key(at), &payload).expect("put");
    }
    store.close().expect("close");
    drop(store);

    // Each fast root opens with a tail of its own; capacity takes nothing fresh.
    for (label, root) in [
        ("home", home.path()),
        ("warm", &warm),
        ("cold", cold.path()),
    ] {
        print!("{label} {}:", root.display());
        for path in segments_in(root) {
            print!(" {}", path.file_name().expect("name").to_string_lossy());
        }
        println!();
    }
    assert!(
        segments_in(cold.path()).is_empty(),
        "a fresh segment landed on capacity"
    );
    assert!(
        home.path().join("reel.volumes").is_file(),
        "no manifest on home"
    );
    for root in [warm.as_path(), cold.path()] {
        assert!(
            root.join("reel.volume").is_file(),
            "no marker on {}",
            root.display()
        );
    }
    println!("manifest on home, marker on each extra root");

    // Segment files plus the list in config are the store, so a sealed segment carried
    // to another configured root keeps serving what it holds.
    let sealed = segments_in(home.path());
    assert!(sealed.len() > 1, "the run sealed nothing to carry");
    let name = sealed[0]
        .file_name()
        .expect("name")
        .to_string_lossy()
        .into_owned();
    std::fs::rename(&sealed[0], warm.join(&name)).expect("carry");

    let whole = ReelStore::open(home.path().to_path_buf(), roots.clone(), COLUMNS).expect("reopen");
    for at in 0..RECORD_COUNT {
        assert!(
            whole.get(&key(at)).expect("get").is_some(),
            "key {at} went missing"
        );
    }
    println!("{name} carried to warm, all {RECORD_COUNT} records still read");
    whole.close().expect("close");
    drop(whole);

    std::fs::remove_dir_all(&warm).expect("lose the drive");
    let refused = ReelStore::open(home.path().to_path_buf(), roots.clone(), COLUMNS);
    assert!(
        refused.is_err(),
        "a lost root opened without the operator's word"
    );
    println!("open refuses a manifest-named root that is gone");

    let mut degraded_config = roots;
    degraded_config.volumes[0] = VolumeSpec::fast(&warm).declared_dead();
    let degraded =
        ReelStore::open(home.path().to_path_buf(), degraded_config, COLUMNS).expect("degraded");
    assert_eq!(degraded.dead_volumes(), vec![warm]);

    let mut missing = 0u64;
    for at in 0..RECORD_COUNT {
        if degraded
            .get(&key(at))
            .expect("a miss, never an error")
            .is_none()
        {
            missing += 1;
        }
    }
    assert!(
        missing > 0,
        "the dead root's records did not read as missing"
    );
    assert!(
        missing < RECORD_COUNT,
        "records the survivors hold went missing too"
    );
    println!("degraded: {missing} of {RECORD_COUNT} records miss, the rest serve");

    degraded
        .put(&key(RECORD_COUNT), &payload)
        .expect("write while degraded");
    assert!(degraded.get(&key(RECORD_COUNT)).expect("get").is_some());
    println!("a write landed while degraded");
}
