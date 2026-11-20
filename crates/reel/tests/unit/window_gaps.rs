use std::fs;
use std::process;
use std::sync::atomic::{AtomicBool, Ordering};

use reel::config::{ColumnConfig, ColumnIndex, KeyShape, StoreConfig, SyncPolicy, ThreadCounts, VolumeConfig};
use reel::engine::reel_store::ReelStore;
use reel::io::plain::PlainIo;
use reel::segment::summary_orders::row_gap_bytes;
use reel_core::column_id::ColumnId;
use reel_core::units::ByteCount;

const COLUMN: ColumnId = ColumnId(1);
const SEGMENT_BYTES: u64 = 4 * 1024 * 1024;
const WRITERS: u64 = 4;
const KEYS: u64 = 40_000;

fn value(number: u64) -> Vec<u8> {
    vec![number as u8 ^ 0x77; 600 + (number % 7) as usize * 100]
}

// writers put single values while another thread seals in a loop, so a seal starts beside a writer refilling its window with
// no claim held. Every value reads back, and no sealed segment keeps a window's unused end as a gap
#[test]
fn single_puts_racing_seals_leave_no_gap() {
    let root = std::env::temp_dir().join(format!("reel-window-gaps-{}", process::id()));
    let _gone = fs::remove_dir_all(&root);
    fs::create_dir_all(&root).expect("root");
    let config = StoreConfig {
        memory_budget: ByteCount(512 * 1024 * 1024),
        threads: ThreadCounts::default(),
        volumes: vec![VolumeConfig {
            open_segments: Some(2),
            segment_bytes: Some(ByteCount(SEGMENT_BYTES)),
            ..VolumeConfig::new("gaps", vec![root.clone()], SyncPolicy::Never)
        }],
        columns: vec![ColumnConfig { index: ColumnIndex::Plain, ..ColumnConfig::new(COLUMN, "gaps", KeyShape::Fixed { width: 8 }) }],
    };
    let store = ReelStore::open(PlainIo::new(), &config).expect("open");
    let done = AtomicBool::new(false);
    std::thread::scope(|scope| {
        let writers: Vec<_> = (0..WRITERS)
            .map(|writer| {
                let store = &store;
                scope.spawn(move || {
                    for number in (0..KEYS).filter(|number| number % WRITERS == writer) {
                        store.put(COLUMN, &number.to_be_bytes(), &value(number)).expect("put");
                    }
                })
            })
            .collect();
        let (store, done) = (&store, &done);
        scope.spawn(move || {
            while !done.load(Ordering::Acquire) {
                store.seal_now();
                std::thread::yield_now();
            }
        });
        for writer in writers {
            writer.join().expect("writer");
        }
        done.store(true, Ordering::Release);
    });
    store.seal_now();

    let mut buf = Vec::new();
    let bad = (0..KEYS).filter(|number| !matches!(store.get(COLUMN, &number.to_be_bytes(), &mut buf), Ok(true) if buf == value(*number))).count();
    assert_eq!(bad, 0, "{bad} of {KEYS} values came back wrong");
    assert_eq!(store.damaged_segments(), 0);

    let volume = &store.state.volumes[0];
    let summary = reel::guard::hold(&volume.summary);
    let gaps: Vec<u64> = summary.rows().map(|row| row_gap_bytes(&row)).collect();
    drop(summary);
    assert!(gaps.len() > 4, "the writes sealed {} segments", gaps.len());
    assert!(gaps.iter().all(|gap| *gap == 0), "a seal kept a gap: {gaps:?}");
    store.close().expect("close");
    let _gone = fs::remove_dir_all(&root);
}
