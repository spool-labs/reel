use std::fs;
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::process;

use reel::config::{ColumnConfig, ColumnIndex, KeyShape, StoreConfig, SyncPolicy, ThreadCounts, VolumeConfig};
use reel::engine::open_segment::{data_path, set_reserve_step};
use reel::engine::reel_store::ReelStore;
use reel::io::plain::PlainIo;
use reel::record::codec::Codec;
use reel_core::column_id::ColumnId;
use reel_core::units::ByteCount;

const COLUMN: ColumnId = ColumnId(1);
const BUDGET: ByteCount = ByteCount(1024 * 1024 * 1024);
const SEGMENT_BYTES: u64 = 16 * 1024 * 1024;
const WRITERS: u64 = 4;
const KEYS: u64 = 192;

fn config(root: &Path, codec: Codec) -> StoreConfig {
    StoreConfig {
        memory_budget: BUDGET,
        threads: ThreadCounts::default(),
        volumes: vec![VolumeConfig {
            open_segments: Some(1),
            segment_bytes: Some(ByteCount(SEGMENT_BYTES)),
            ..VolumeConfig::new("large", vec![root.to_path_buf()], SyncPolicy::EveryBytes(ByteCount(SEGMENT_BYTES)))
        }],
        columns: vec![ColumnConfig { codec, index: ColumnIndex::Plain, ..ColumnConfig::new(COLUMN, "large", KeyShape::Fixed { width: 8 }) }],
    }
}

// a value from a segment's eighth to its quarter, which passes the room a reserve step holds
fn size(number: u64) -> usize {
    let eighth = SEGMENT_BYTES / 8;
    (eighth + number * 97 * 1024 % eighth) as usize
}

// bytes lz4 can't shrink, so every codec stores them whole
fn noise(number: u64) -> Vec<u8> {
    let mut state = number.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    let mut value = Vec::with_capacity(size(number) + 8);
    while value.len() < size(number) {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        value.extend_from_slice(&state.to_le_bytes());
    }
    value.truncate(size(number));
    value
}

// the bytes a file holds on disk, and its length rounded up to the filesystem's block
fn held_and_rounded(path: &Path) -> (u64, u64) {
    let meta = fs::metadata(path).expect("a segment file");
    (meta.blocks() * 512, meta.len().next_multiple_of(meta.blksize()))
}

// four writers put values of a segment's eighth to its quarter into one open segment, where each passes the reserved room and
// the reserve runs beside the others' writes, and every value reads back whole. Once a reserve set the file's length and cut
// off the tail another writer had just written past it
fn writers_keep_their_tails(codec: Codec) {
    let root = std::env::temp_dir().join(format!("reel-large-writers-{codec:?}-{}", process::id()));
    let _gone = fs::remove_dir_all(&root);
    fs::create_dir_all(&root).expect("root");
    let store = ReelStore::open(PlainIo::new(), &config(&root, codec)).expect("open");
    std::thread::scope(|scope| {
        for writer in 0..WRITERS {
            let store = &store;
            scope.spawn(move || {
                for number in (0..KEYS).filter(|number| number % WRITERS == writer) {
                    store.put(COLUMN, &number.to_be_bytes(), &noise(number)).expect("put");
                }
            });
        }
    });

    let mut buf = Vec::new();
    let bad: Vec<String> = (0..KEYS)
        .filter_map(|number| match store.get(COLUMN, &number.to_be_bytes(), &mut buf) {
            Ok(true) if buf == noise(number) => None,
            outcome => Some(format!("key {number} of {} B: {outcome:?}", size(number))),
        })
        .collect();
    assert!(bad.is_empty(), "{codec:?}: {} of {KEYS} values came back wrong: {bad:?}", bad.len());

    let volume = &store.state.volumes[0];
    let mut sealed = Vec::new();
    volume.table.for_each_segment(|number, _root, _file| sealed.push(number));
    assert!(sealed.len() > 4, "{codec:?}: the writes sealed {} segments", sealed.len());
    store.close().expect("close");

    // a sealed segment gives back every block past its end
    for number in &sealed {
        let (held, rounded) = held_and_rounded(&data_path(&root, *number));
        assert!(held <= rounded, "{codec:?}: sealed segment {number:?} holds {held} B past its length rounded to {rounded}");
    }
    // the open segment holds at most a step and a half past its writes, since the reserve job asks when half a step is left and
    // takes a step, and a spare holds a step
    let step = set_reserve_step(ByteCount(SEGMENT_BYTES));
    let ahead = step + step / 2;
    for entry in fs::read_dir(&root).expect("read the root") {
        let path = entry.expect("an entry").path();
        if path.extension().is_some_and(|extension| extension == "data") {
            let (held, rounded) = held_and_rounded(&path);
            assert!(held <= rounded + ahead, "{codec:?}: {} holds {held} B, past its length {rounded} and {ahead} ahead", path.display());
        }
    }
    let _gone = fs::remove_dir_all(&root);
}

#[test]
fn uncoded_writers_keep_their_tails() {
    writers_keep_their_tails(Codec::None);
}

#[test]
fn lz4_writers_keep_their_tails() {
    writers_keep_their_tails(Codec::Lz4);
}
