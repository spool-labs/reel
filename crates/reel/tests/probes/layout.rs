//! What a reel directory looks like on disk, printed rather than asserted
//!
//! Builds a two-volume store, rolls enough segments that the naming is visible, takes
//! a checkpoint, and prints the tree each root holds. The roots are left under
//! `target/reel` rather than in a temp dir the run deletes, so the tree can be walked
//! after the fact.
//!
//! Opt-in. Run with:
//!   cargo test -p tape-reel --test probes -- layout

use std::path::{Path, PathBuf};

use reel::config::{ReelConfig, SyncPolicy, ThreadBudget, VolumeSpec};
use reel::format::column::{Codec, ColumnId, ColumnSet, ColumnSpec, MapShape, RecordKey};
use reel::units::ByteCount;
use reel::{KeyWidth, Preallocate, ReelStore};

const ROWS: ColumnId = ColumnId(1);

const COLUMNS: ColumnSet = &[ColumnSpec {
    id: ROWS,
    name: "rows",
    key_width: KeyWidth::Fixed(16),
    shard_bytes: 0,
    inline_max: 0,
    row_carry: 0,
    purge_mark: None,
    codec: Codec::None,
    map_shape: MapShape::Tree,
}];

/// A fresh directory under the build's target dir, emptied if a run left one
///
/// Not a temp dir: the listing is the point, so the roots outlive the test.
fn root(name: &str) -> PathBuf {
    let target = match std::env::var_os("CARGO_TARGET_DIR") {
        Some(set) => PathBuf::from(set),
        None => Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("..")
            .join("target"),
    };
    let path = target.join("reel").join(name);
    if path.exists() {
        std::fs::remove_dir_all(&path).expect("clear a previous run");
    }
    std::fs::create_dir_all(&path).expect("create root");
    // The manifest-relative walk up leaves `../..` in the middle, and these paths are
    // printed for a person to paste.
    path.canonicalize().unwrap_or(path)
}

fn key(at: u64) -> RecordKey {
    let mut bytes = [0u8; 16];
    bytes[8..].copy_from_slice(&at.to_be_bytes());
    RecordKey::from_bytes(ROWS, &bytes).expect("key")
}

/// Print one directory, one line per entry, sizes in bytes
fn tree(label: &str, root: &Path) {
    println!("\n{label}  ({})", root.display());
    let mut found: Vec<PathBuf> = std::fs::read_dir(root)
        .expect("read root")
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .collect();
    found.sort();
    for path in found {
        let name = path
            .file_name()
            .expect("name")
            .to_string_lossy()
            .to_string();
        let meta = std::fs::metadata(&path).expect("stat");
        match meta.is_dir() {
            true => {
                println!("  {name}/");
                let mut inner: Vec<PathBuf> = std::fs::read_dir(&path)
                    .expect("read dir")
                    .filter_map(|entry| entry.ok())
                    .map(|entry| entry.path())
                    .collect();
                inner.sort();
                for child in inner {
                    let child_name = child
                        .file_name()
                        .expect("name")
                        .to_string_lossy()
                        .to_string();
                    let bytes = std::fs::metadata(&child).expect("stat").len();
                    println!("    {child_name:<20} {bytes:>10} B");
                }
            }
            false => println!("  {name:<22} {:>10} B", meta.len()),
        }
    }
}

pub fn what_a_volume_holds() {
    let home = root("home");
    let extra = root("extra");

    let config = ReelConfig {
        segment_bytes: ByteCount::from_bytes(256 * 1024),
        alloc_chunk: ByteCount::from_bytes(64 * 1024),
        preallocate: Preallocate::Chunk,
        sync: SyncPolicy::Never,
        active_tails: ThreadBudget::threads(2),
        volumes: vec![VolumeSpec::fast(extra.clone())],
        ..ReelConfig::default()
    };

    let store = ReelStore::open(home.clone(), config, COLUMNS).expect("open");
    let payload = vec![0x5Au8; 8 * 1024];
    for at in 0..400u64 {
        store.put(&key(at), &payload).expect("put");
    }

    let copy = home.join("backup-0001");
    let taken = store.checkpoint(&copy).expect("checkpoint");

    tree("home volume, store open", &home);
    tree("second volume, store open", &extra);
    tree("checkpoint on home", &copy);
    tree("checkpoint on second volume", &extra.join("backup-0001"));

    store.close().expect("close");
    drop(store);

    println!("\nafter close");
    tree("home volume, store closed", &home);
    println!(
        "\ncheckpoint stands at sequence {:?} over {} segments",
        taken.at, taken.segments
    );
    println!(
        "the roots are left at {}",
        home.parent().expect("parent").display()
    );
}
