//! Whole-segment reclaim: two groups dropped by range, then taken back by unlink
//!
//! Records are written a group at a time under a shared key prefix, so a group's
//! records sit together in the log. Dropping two of them costs two range records and
//! leaves whole segments holding nothing live, which compaction retires by unlinking
//! the file rather than copying survivors out of it.
//!
//! cargo run --example cohorts

use std::thread::sleep;
use std::time::Duration;

use tempfile::TempDir;

use reel::{
    ByteCount, Codec, ColumnId, ColumnSet, ColumnSpec, CompactPass, CompactRate, KeyWidth,
    MapShape, Preallocate, RecordKey, ReelConfig, ReelStore, SyncPolicy, ThreadBudget,
};

const EVENTS: ColumnId = ColumnId(1);

/// Segments small enough that one group spans several, so a drop empties files
const SEGMENT_BYTES: ByteCount = ByteCount::from_bytes(128 * 1024);
const ALLOC_CHUNK: ByteCount = ByteCount::from_bytes(32 * 1024);
const PAYLOAD_BYTES: usize = 4 * 1024;
const GROUP_COUNT: u32 = 6;
const PER_GROUP: u64 = 64;

/// The two groups dropped, and the ceiling on passes driving their space back
const DROPPED: [u32; 2] = [2, 3];
const MAX_PASSES: u32 = 200;
const SETTLE: Duration = Duration::from_millis(10);

const COLUMNS: ColumnSet = &[ColumnSpec {
    id: EVENTS,
    name: "events",
    key_width: KeyWidth::Fixed(12),
    shard_bytes: 0,
    inline_max: 0,
    row_carry: 0,
    purge_mark: None,
    codec: Codec::None,
    map_shape: MapShape::Tree,
}];

/// Group in the leading four bytes, sequence in the rest, so a group is one range
fn key(group: u32, at: u64) -> RecordKey {
    let mut bytes = [0u8; 12];
    bytes[..4].copy_from_slice(&group.to_be_bytes());
    bytes[4..].copy_from_slice(&at.to_be_bytes());
    RecordKey::from_bytes(EVENTS, &bytes).expect("key")
}

/// The lowest key of a group, which is the exclusive bound of the one before it
fn group_bound(group: u32) -> [u8; 12] {
    let mut bytes = [0u8; 12];
    bytes[..4].copy_from_slice(&group.to_be_bytes());
    bytes
}

fn main() {
    let root = TempDir::new().expect("root");
    let config = ReelConfig {
        segment_bytes: SEGMENT_BYTES,
        alloc_chunk: ALLOC_CHUNK,
        preallocate: Preallocate::Chunk,
        sync: SyncPolicy::Never,
        active_tails: ThreadBudget::threads(1),
        compact_mbps: CompactRate::Mbps(0),
        scrub_mbps: 0,
        ..ReelConfig::default()
    };
    let store = ReelStore::open(root.path().to_path_buf(), config, COLUMNS).expect("open");

    let payload = vec![0x5Au8; PAYLOAD_BYTES];
    for group in 0..GROUP_COUNT {
        for at in 0..PER_GROUP {
            store.put(&key(group, at), &payload).expect("put");
        }
    }
    let written = store.totals();
    let live_bytes = written.bytes.to_bytes();
    println!(
        "{} records in {GROUP_COUNT} groups, {live_bytes} live bytes",
        written.count
    );

    for group in DROPPED {
        let start = RecordKey::from_bytes(EVENTS, &group_bound(group)).expect("key");
        store
            .delete_range(&start, Some(&group_bound(group + 1)))
            .expect("drop the group");
    }
    // The bytes a range covers reach the dead gauge on the sweep, so a caller wanting
    // the whole total runs it out.
    while store.sweep_covers().expect("sweep") {}

    let dead_before = store.dead_bytes().to_bytes();
    let live = store.totals().count;
    assert_eq!(live, written.count - DROPPED.len() as u64 * PER_GROUP);
    println!("groups {DROPPED:?} dropped by range, {live} records live, {dead_before} dead bytes");

    let mut passes = 0u32;
    for _ in 0..MAX_PASSES {
        if store.dead_bytes().to_bytes() * 4 <= dead_before {
            break;
        }
        if store.compact_once().expect("compact") == CompactPass::Copied {
            passes += 1;
        }
        // Compaction only takes a segment its sealer has settled, and the sealers
        // run on their own threads, so the loop waits rather than counting tries.
        sleep(SETTLE);
    }

    assert!(passes > 0, "no pass did any work");
    let dead_after = store.dead_bytes().to_bytes();
    let counters = store.compaction_counters();
    println!("dead bytes {dead_before} -> {dead_after}");
    println!(
        "{} segments unlinked whole, {} rewritten, {} bytes copied",
        counters.segments_unlinked_whole, counters.segments_rewritten, counters.compaction_bytes,
    );
    assert!(
        dead_after * 4 <= dead_before,
        "the dropped space did not come back"
    );
    assert!(
        counters.segments_unlinked_whole > counters.segments_rewritten,
        "reclaim copied its way through more segments than it unlinked"
    );
    assert!(
        counters.compaction_bytes * 4 < dead_before - dead_after,
        "the bytes copied are the same order as the bytes reclaimed"
    );
    println!(
        "{:.0}% of retirements were a plain unlink",
        (1.0 - counters.move_ratio()) * 100.0
    );

    for group in 0..GROUP_COUNT {
        let found = store
            .get(&key(group, PER_GROUP - 1))
            .expect("get")
            .is_some();
        assert_eq!(
            found,
            !DROPPED.contains(&group),
            "group {group} answered wrongly"
        );
    }
    println!("every surviving group still reads, both dropped groups miss");
}
