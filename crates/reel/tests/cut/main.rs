//! Real threads: a cue across two volumes never holds a later write without the earlier one it followed

use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use reel::config::{ColumnConfig, KeyShape, StoreConfig, SyncPolicy, VolumeConfig};
use reel::engine::reel_store::ReelStore;
use reel::io::plain::PlainIo;
use reel::record::codec::Codec;
use reel_api::column_id::ColumnId;
use reel_api::units::{ByteCount, VolumeId};

const FIRST: ColumnId = ColumnId(1);
const SECOND: ColumnId = ColumnId(2);
const BUDGET: ByteCount = ByteCount(256 * 1024 * 1024);
const RUN: Duration = Duration::from_secs(3);
const KEY: &[u8] = b"count";

fn config(root: &PathBuf) -> StoreConfig {
    let volume = |name: &str| VolumeConfig {
        name: name.to_string(),
        roots: vec![root.join(name)],
        sync: SyncPolicy::Never,
        device: Default::default(),
    };
    let column = |id: ColumnId, name: &str, volume: u8| ColumnConfig {
        id,
        name: name.to_string(),
        key_shape: KeyShape::Variable { most: 16 },
        codec: Codec::None,
        volume: VolumeId(volume),
        complete_rows: None,
    };
    StoreConfig {
        memory_budget: BUDGET,
        threads: Default::default(),
        volumes: vec![volume("first"), volume("second")],
        columns: vec![column(FIRST, "first", 0), column(SECOND, "second", 1)],
    }
}

fn count_of(value: &[u8]) -> u64 {
    let mut word = [0; size_of::<u64>()];
    word.copy_from_slice(value);
    u64::from_le_bytes(word)
}

// each round writes the first volume and then the second, so a cut can only show the first at or past the second
#[test]
fn cue_never_shows_the_later_write_alone() {
    let root = std::env::temp_dir().join(format!("reel-cut-{}", std::process::id()));
    let _gone = fs::remove_dir_all(&root);
    let store = ReelStore::open(PlainIo::new(), &config(&root)).expect("open");
    let is_done = AtomicBool::new(false);
    let cues = thread::scope(|scope| {
        scope.spawn(|| {
            let mut count = 0_u64;
            while !is_done.load(Ordering::Acquire) {
                count += 1;
                store.put(FIRST, KEY, &count.to_le_bytes()).expect("put");
                store.put(SECOND, KEY, &count.to_le_bytes()).expect("put");
            }
        });
        let start = Instant::now();
        let (mut first, mut second, mut cues) = (Vec::new(), Vec::new(), 0_u64);
        while start.elapsed() < RUN {
            let cue = store.cue().expect("cue");
            let has_second = cue.get(SECOND, KEY, &mut second).expect("get");
            let has_first = cue.get(FIRST, KEY, &mut first).expect("get");
            if has_second {
                assert!(has_first);
                assert!(count_of(&first) >= count_of(&second), "{} below {}", count_of(&first), count_of(&second));
            }
            cues += 1;
        }
        is_done.store(true, Ordering::Release);
        cues
    });
    assert!(cues > 0);
    store.close().expect("close");
    fs::remove_dir_all(&root).expect("remove");
}
