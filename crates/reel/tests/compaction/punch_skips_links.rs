//! A punch leaves a segment file alone while another name links it, as a checkpoint does

use std::path::{Path, PathBuf};

use reel::{
    ByteCount, Codec, ColumnId, ColumnSet, ColumnSpec, KeyWidth, ReelConfig, ReelStore, SyncPolicy,
    ThreadBudget,
};
use reel_core::Store;

const COLUMNS: ColumnSet = &[ColumnSpec {
    id: ColumnId(1),
    name: "rows",
    key_width: KeyWidth::Fixed(8),
    shard_bytes: 0,
    purge_mark: None,
    codec: Codec::None,
}];

const VALUE: usize = 64 * 1024;

// link the sealed segment, and the punch gives nothing back until the link is gone
#[test]
fn a_linked_segment_is_never_punched() {
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path().join("volume");
    let store = ReelStore::open(
        root.clone(),
        ReelConfig {
            segment_bytes: ByteCount::mb(16),
            sync: SyncPolicy::Never,
            active_tails: ThreadBudget::threads(1),
            ..ReelConfig::default()
        },
        COLUMNS,
    )
    .expect("open");
    for at in 0..300u64 {
        Store::put(&store, "rows", &at.to_be_bytes(), &[at as u8; VALUE]).expect("put");
    }
    for at in 0..70u64 {
        Store::put(&store, "rows", &at.to_be_bytes(), &[!(at as u8); VALUE]).expect("overwrite");
    }
    store.flush().expect("flush");

    let sealed = first_segment(&root);
    let link = dir.path().join("checkpoint.reel");
    std::fs::hard_link(&sealed, &link).expect("link");
    let linked = store.erase_dead_runs().expect("punch");
    assert_eq!(linked.erased_bytes, 0, "a linked file was punched");

    std::fs::remove_file(&link).expect("unlink");
    let unlinked = store.erase_dead_runs().expect("punch");
    assert!(
        unlinked.erased_bytes >= 4 << 20,
        "the unlinked segment gave back only {} bytes",
        unlinked.erased_bytes
    );
    for at in [0u64, 69, 70, 299] {
        let want = if at < 70 { !(at as u8) } else { at as u8 };
        let value = Store::get(&store, "rows", &at.to_be_bytes()).expect("get");
        assert_eq!(
            value.as_deref(),
            Some(vec![want; VALUE].as_slice()),
            "key {at}"
        );
    }
}

/// The lowest numbered segment file, the one the first writes sealed
fn first_segment(root: &Path) -> PathBuf {
    let mut files: Vec<PathBuf> = std::fs::read_dir(root)
        .expect("read volume")
        .flatten()
        .map(|entry| entry.path())
        .filter(|path| path.extension().is_some_and(|suffix| suffix == "reel"))
        .collect();
    files.sort();
    files.into_iter().next().expect("a segment file")
}
