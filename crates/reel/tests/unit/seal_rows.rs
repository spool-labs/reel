use std::fs;
use std::process;

use reel::config::{ColumnConfig, ColumnIndex, KeyShape, StoreConfig, SyncPolicy, ThreadCounts, VolumeConfig};
use reel::engine::engine_sizing::rows_within_blocks;
use reel::engine::reel_store::ReelStore;
use reel::io::plain::PlainIo;
use reel::record::codec::Codec;
use reel_core::column_id::ColumnId;
use reel_core::units::ByteCount;

const MIB: u64 = 1024 * 1024;

// a store of every key width at every budget, its segment size set or left to the budget, opens with a row limit its seal's
// list holds. A wide key once took the budget's limit and the open refused it. Run with --nocapture for the limits each gets
#[test]
fn every_width_and_budget_opens_within_its_list() {
    let shapes = [KeyShape::Fixed { width: 16 }, KeyShape::Variable { most: 64 }, KeyShape::Variable { most: 255 }, KeyShape::Variable { most: 1_056 }];
    for shape in shapes {
        for codec in [Codec::None, Codec::Lz4] {
            for budget in [128 * MIB, 1024 * MIB, 2048 * MIB, 8192 * MIB] {
                for segment in [None, Some(ByteCount(32 * MIB))] {
                    let root = std::env::temp_dir().join(format!("reel-seal-rows-{}", process::id()));
                    let _gone = fs::remove_dir_all(&root);
                    fs::create_dir_all(&root).expect("root");
                    let config = StoreConfig {
                        memory_budget: ByteCount(budget),
                        max_memory: None,
                        threads: ThreadCounts::default(),
                        volumes: vec![VolumeConfig { segment_bytes: segment, ..VolumeConfig::new("rows", vec![root.clone()], SyncPolicy::Never) }],
                        columns: vec![ColumnConfig { codec, index: ColumnIndex::Plain, ..ColumnConfig::new(ColumnId(1), "rows", shape) }],
                    };
                    let cell = format!("{shape:?} {codec:?} {} MiB, segment {:?} MiB", budget / MIB, segment.map(|bytes| bytes.0 / MIB));
                    let store = ReelStore::open(PlainIo::new(), &config).unwrap_or_else(|error| panic!("{cell}: {error}"));
                    let volume = &store.state.volumes[0];
                    let listed = rows_within_blocks(volume.seal_blocks, shape.widest());
                    println!("{cell}: row limit {}, list of {} blocks holds {listed}", volume.segment_rows.0, volume.seal_blocks.0);
                    assert!(volume.segment_rows.0 <= listed, "{cell}: a segment takes {} rows and its list holds {listed}", volume.segment_rows.0);
                    store.close().expect("close");
                    let _gone = fs::remove_dir_all(&root);
                }
            }
        }
    }
}
