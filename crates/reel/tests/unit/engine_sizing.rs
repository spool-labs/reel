use std::path::PathBuf;

use reel::config::{ColumnConfig, DeviceClass, KeyShape, NO_WORKERS, StoreConfig, SyncPolicy, ThreadCounts, VolumeConfig};
use reel::engine::engine_sizing::*;
use reel::engine::reel_store::{planned_table, store_sizing, volume_entries};
use reel::engine::seal_job::MOST_SEGMENT_BLOCKS;
use reel::index::open_slots::MAX_OPEN_SLOTS;
use reel::io::plain_file::PlainFile;
use reel::record::codec::Codec;
use reel::row_kind::RowKind;
use reel::segment::column_directory::ColumnDirectory;
use reel::segment::row_block::{COMPLETE_BLOCK_BYTES, FooterRow, RowBlockBuilder};
use reel::segment::segment_table::{NumberMap, SegmentTable, TABLE_SHARDS, TableSizing};
use reel_api::column_id::ColumnId;
use reel_api::units::{ByteCount, FileOffset, RowCount, VolumeId, WritePlace};

const ROWS_PER_BLOCK: u64 = 1_400;
const MOST_ROW_BYTES: u64 = 1_024;
const BOX_DISK_BYTES: u64 = 241_566_183_424;
const BOX_CHURN_SEGMENTS: usize = 421;

// the default budget gets near-gigabyte segments, and a small one scales down
#[test]
fn budget_split() {
    let full = EngineSizing::for_store(&shape(FULL_BUDGET, 3));
    let small = EngineSizing::for_store(&shape(ByteCount(64 * 1024 * 1024), 1));

    assert_eq!(full.segment_bytes, SEGMENT_TARGET);
    assert_eq!(full.lz4_stored_limit, LZ4_STORED_LIMIT);
    assert!(small.segment_bytes < full.segment_bytes);
    assert!(small.shard.open_slots <= MAX_OPEN_SLOTS);
    assert!(small.segment_rows.0 > 0);
}

fn shape(budget: ByteCount, roots: usize) -> StoreShape {
    StoreShape {
        budget,
        roots,
        volumes: 1,
        has_lz4: false,
        widest_keys: 64,
        threads: reel::config::NO_WORKERS,
        has_ring: false,
        disk_entry_bytes: ByteCount(0),
        floor_entry_bytes: ByteCount(0),
        workers: None,
        declared_bytes: ByteCount(0),
    }
}

// lz4's compress buffer counts against the budget, and the floor rises with it
#[test]
fn least_budget() {
    let plain = shape(FULL_BUDGET, 1);
    let lz4 = StoreShape { has_lz4: true, ..plain };

    assert!(plain.least_budget() < LEAST_LZ4_BUDGET);
    assert_eq!(lz4.least_budget().0, plain.least_budget().0 + LZ4_STORED_LIMIT.0);
}

// the rows a segment takes at each budget, as `SEAL_ROWS_EIGHTHS` documents them
#[test]
fn documented_rows() {
    for (mib, rows) in [(128, 12_317), (192, 91_505), (512, 487_443), (4_096, 4_921_950)] {
        let store = StoreShape {
            budget: ByteCount(mib * 1024 * 1024),
            roots: 1,
            volumes: 1,
            has_lz4: true,
            widest_keys: 64,
            threads: reel::config::NO_WORKERS,
            has_ring: false,
            disk_entry_bytes: ByteCount(0),
            floor_entry_bytes: ByteCount(0),
            workers: None,
            declared_bytes: ByteCount(0),
        };

        assert_eq!(EngineSizing::for_store(&store).segment_rows, RowCount(rows), "{mib} MiB");
    }
}

// a summary at the default budget holds the block counts of a billion keys, beside a row for each segment
#[test]
fn summary_holds_blocks() {
    let store = StoreShape {
        budget: ByteCount(512 * 1024 * 1024),
        roots: 1,
        volumes: 1,
        has_lz4: true,
        widest_keys: 64,
        threads: reel::config::NO_WORKERS,
        has_ring: false,
        disk_entry_bytes: ByteCount(0),
        floor_entry_bytes: ByteCount(0),
        workers: None,
        declared_bytes: ByteCount(0),
    };
    let rows_room = EngineSizing::for_store(&store).summary_share.0 / 2;
    let blocks = 1_000_000_000 / ROWS_PER_BLOCK;
    let segments = 1_024;

    assert!(rows_room > blocks * size_of::<u32>() as u64 + segments * MOST_ROW_BYTES);
}

// the reserve an idle volume holds at each budget, as `SEAL_ROWS_EIGHTHS` documents it
#[test]
fn documented_reserve() {
    for (mib, reserve) in [(128, 128), (192, 192), (512, 2_048), (4_096, 2_048)] {
        let store = StoreShape {
            budget: ByteCount(mib * 1024 * 1024),
            roots: 1,
            volumes: 1,
            has_lz4: true,
            widest_keys: 64,
            threads: reel::config::NO_WORKERS,
            has_ring: false,
            disk_entry_bytes: ByteCount(0),
            floor_entry_bytes: ByteCount(0),
            workers: None,
            declared_bytes: ByteCount(0),
        };

        let segment = EngineSizing::for_store(&store).segment_bytes;
        assert_eq!(2 * segment.0, reserve * 1024 * 1024, "{mib} MiB");
    }
}

fn blocks_for(rows: u64, row_of: impl Fn(u64) -> (Vec<u8>, FooterRow)) -> u64 {
    let mut builder = RowBlockBuilder::new();
    builder.set_block_bytes(COMPLETE_BLOCK_BYTES);
    let mut blocks = 1;
    for number in 0..rows {
        let (key, row) = row_of(number);
        if !builder.push(&key, &row) {
            let _bytes = builder.finish();
            builder.clear();
            blocks += 1;
            assert!(builder.push(&key, &row));
        }
    }
    blocks
}

// a seal's row limit fits the block lines a slot can address, with the widest rows and the smallest the format allows
#[test]
fn complete_rows_fit_the_lines() {
    let widest = 32;
    let rows = complete_row_limit(widest).0;
    let worst = |number: u64| {
        let mut key = vec![0; widest];
        key[..8].copy_from_slice(&number.wrapping_mul(0x9E37_79B9_7F4A_7C15).to_be_bytes());
        let row = FooterRow {
            place: WritePlace(u64::MAX - number),
            offset: FileOffset(u64::from(u32::MAX) - number),
            length: ByteCount(u64::from(u32::MAX)),
            kind: RowKind::Value,
        };
        (key, row)
    };
    assert!(blocks_for(rows, worst) <= u64::from(MOST_SEGMENT_BLOCKS.0));

    let smallest = |number: u64| {
        let row = FooterRow {
            place: WritePlace(0),
            offset: FileOffset(0),
            length: ByteCount(0),
            kind: RowKind::Grave,
        };
        (number.to_be_bytes().to_vec(), row)
    };
    assert!(blocks_for(rows, smallest) <= u64::from(MOST_SEGMENT_BLOCKS.0));
}

// a seal point leaves room for what arrives while the seal runs, and stays inside the row limit and its floor
#[test]
fn seal_point_from_rates() {
    let limit = RowCount(5_000_000);
    let index_rows = limit.0 * 8 / SEAL_ROWS_EIGHTHS;
    assert_eq!(seal_point(limit, 0, 1_000_000), limit);
    assert_eq!(seal_point(limit, 1_000_000, 0), limit);

    let (seal_rate, ingest_rate) = (2_000_000, 1_600_000);
    let point = seal_point(limit, seal_rate, ingest_rate).0;
    assert!(point < limit.0);
    let seal_seconds = point as f64 / seal_rate as f64;
    let room_seconds = (index_rows - point) as f64 / ingest_rate as f64;
    assert!(room_seconds >= seal_seconds, "{room_seconds} s of room, {seal_seconds} s of seal");

    assert_eq!(seal_point(limit, 100_000_000, 1), limit);
    assert_eq!(seal_point(limit, 1, 100_000_000).0, index_rows / LEAST_SEAL_DIVISOR);

    // a seal starts with room for a tenth of a second of its own rate, whatever the ingest
    let (fast_seal, slow_ingest) = (6_000_000, 1);
    assert!(index_rows - seal_point(limit, fast_seal, slow_ingest).0 >= fast_seal / 10);
}

// a planned entry covers what the table, each column's directory, the volume's spare, the summary's lookup and the covered list hold
// for a segment
#[test]
fn planned_entries_cover_the_heap() {
    for columns in [1, 2, 4] {
        for per_shard in [8, 100, 1_700, 1_848] {
            let segments = per_shard * TABLE_SHARDS;
            let sizing = TableSizing { segments_per_shard: per_shard, columns_per_shard: per_shard * columns, blocks_per_shard: 0 };
            let table: SegmentTable<PlainFile> = SegmentTable::new(&sizing);
            let directories = (columns as u64 + 1) * ColumnDirectory::new(segments).heap_bytes().0;
            let lookup = NumberMap::new(segments).heap_bytes() as u64;
            let covered = (2 * segments * size_of::<u64>()) as u64;
            let held = table.heap_bytes().0 + directories + lookup + covered;

            assert!(held <= segments as u64 * entry_cost(columns), "{columns} columns at {per_shard} a shard hold {held} B");
        }
    }
}

// a disk sets a volume's entries at twice the segments it holds in whole shards. The least budget rises by the tables' floor
// alone, the line comes out of the shares, and a small budget caps the line at a quarter of what it has past its least
#[test]
fn disk_sets_the_entries() {
    let hz02 = ByteCount(3_875_398_791 * 4_096);
    let bare = shape(FULL_BUDGET, 1);
    let asked = ByteCount(29_568 * entry_cost(2));
    let floor = ByteCount(floor_entries() as u64 * entry_cost(2));
    let disked = StoreShape { disk_entry_bytes: asked, floor_entry_bytes: floor, ..bare };
    let small = StoreShape { budget: ByteCount(128 * 1024 * 1024), ..disked };
    let small_cap = (small.budget.0 - small.least_budget().0) / ENTRY_LINE_DIVISOR;

    assert_eq!(disk_entries(Some(hz02), SEGMENT_TARGET), Some(29_568));
    assert_eq!(disk_entries(None, SEGMENT_TARGET), None);
    assert_eq!(disk_entries(Some(ByteCount(1)), SEGMENT_TARGET), Some(floor_entries()));
    // each order entry holds its record bytes and the bytes freed in place, which the space target reads
    assert_eq!(asked, ByteCount(55_706_112));
    assert_eq!(disked.least_budget().0, bare.least_budget().0 + floor.0);
    assert_eq!(disked.entry_line(), asked);
    assert!(EngineSizing::for_store(&disked).table_share < EngineSizing::for_store(&bare).table_share);
    assert_eq!(small.entry_line(), ByteCount(floor.0 + small_cap));
    assert!(small.entry_line() < asked);
}

// a capped volume keeps its floor and its part of the room past it in whole shards, and its cover stops at the segments its
// entries plan for or the disk, whichever is smaller
#[test]
fn capped_entries_and_cover() {
    let segment = ByteCount(64 * 1024 * 1024);

    assert_eq!(capped_entries(29_648, 1_000, 4_000), 7_504);
    assert_eq!(capped_entries(200, 0, 4_000), floor_entries());
    assert_eq!(entry_cover(7_504, segment, ByteCount(1 << 40)), ByteCount(3_752 * segment.0));
    assert_eq!(entry_cover(7_504, segment, ByteCount(1 << 30)), ByteCount(1 << 30));
}

// the box's churn at 512 MiB on its 241 GB disk held 421 segments at its most, runs and row-limit seals far under the byte
// target, so every shard has room for all of them and the churn can never fill one and stop a seal
#[test]
fn box_churn_fits_the_table() {
    let column = |id: u8, name: &str, codec: Codec| ColumnConfig {
        id: ColumnId(id),
        name: name.to_string(),
        key_shape: KeyShape::Variable { most: 64 },
        codec,
        volume: VolumeId(0),
        complete_rows: None,
        sorted_rows: None,
        least_value: None,
    };
    let config = StoreConfig {
        memory_budget: FULL_BUDGET,
        threads: ThreadCounts { maintenance_workers: Some(2), ..NO_WORKERS },
        volumes: vec![VolumeConfig { name: "smoke".to_string(), roots: vec![PathBuf::from("/box")], sync: SyncPolicy::Never, device: DeviceClass::default() }],
        columns: vec![column(1, "meta", Codec::None), column(2, "bulk", Codec::Lz4)],
    };
    let disks = [Some(ByteCount(BOX_DISK_BYTES))];
    let sizing = store_sizing(&config, true, &disks).expect("sizing");
    let entries = volume_entries(&config, &disks, sizing.segment_rows, sizing.entry_line);
    let table = planned_table(&config, entries[0], 0, sizing.table_share.0);

    assert!(table.segments_per_shard >= BOX_CHURN_SEGMENTS, "{} entries a shard", table.segments_per_shard);
}

const SMALL_RECORD: u64 = 120;
const WIDE_RECORD: u64 = 4_112;
const TWO_GIB: ByteCount = ByteCount(2 * 1024 * 1024 * 1024);

// a store of 4 KiB records needs a far smaller write index, and the record cache takes what it gives up, with the total held
#[test]
fn wide_records_shrink_the_index() {
    let planned = EngineSizing::for_store(&shape(TWO_GIB, 1));
    let wide = planned.for_mean_record(Some(WIDE_RECORD), 1);
    assert!(wide.segment_rows < planned.segment_rows, "the index kept {:?} rows for wide records", wide.segment_rows);
    let planned_rows = planned.segment_rows.0 * 8 / SEAL_ROWS_EIGHTHS;
    let wanted = (planned.segment_bytes.0.div_ceil(WIDE_RECORD) * 8 / SEAL_ROWS_EIGHTHS * INDEX_ROWS_MARGIN).max(planned_rows / LEAST_INDEX_DIVISOR);
    assert_eq!(wide.segment_rows.0, wanted * SEAL_ROWS_EIGHTHS / 8);
    let freed = (planned.shard.heap_bytes().0 - wide.shard.heap_bytes().0) * reel::index::write_index::SHARD_COUNT as u64;
    assert_eq!(wide.record_share.0, planned.record_share.0 + freed, "the record cache took other than what the index freed");
    assert!(wide.shard.table_slots <= planned.shard.table_slots);
}

// small records, or a store with nothing on disk, keep the index as planned
#[test]
fn small_records_keep_the_index() {
    let planned = EngineSizing::for_store(&shape(TWO_GIB, 1));
    assert_eq!(planned.for_mean_record(Some(SMALL_RECORD), 1), planned);
    assert_eq!(planned.for_mean_record(None, 1), planned);
    assert_eq!(planned.for_mean_record(Some(0), 1), planned);
}

const HUGE_RECORD: u64 = 64 * 1024;

// the seal point's floor, a quarter of the index, stays past a segment of the mean record, so the byte target seals first
#[test]
fn shrunk_index_seals_by_bytes() {
    let planned = EngineSizing::for_store(&shape(TWO_GIB, 1));
    let wide = planned.for_mean_record(Some(WIDE_RECORD), 1);
    let index_rows = wide.segment_rows.0 * 8 / SEAL_ROWS_EIGHTHS;
    assert!(index_rows / LEAST_SEAL_DIVISOR > planned.segment_bytes.0 / WIDE_RECORD, "the seal floor undercuts a full segment");
}

// a store of very wide records keeps a quarter of the planned index, so small rows after the open still seal at a fair size
#[test]
fn shrink_keeps_a_quarter() {
    let planned = EngineSizing::for_store(&shape(TWO_GIB, 1));
    let huge = planned.for_mean_record(Some(HUGE_RECORD), 1);
    let planned_rows = planned.segment_rows.0 * 8 / SEAL_ROWS_EIGHTHS;
    assert_eq!(huge.segment_rows.0, planned_rows / LEAST_INDEX_DIVISOR * SEAL_ROWS_EIGHTHS / 8);
    assert!(huge.record_share > planned.record_share);
}

// a shard's sizing reads the heap the built shard holds, so what the index frees is what it really gave up
#[test]
fn shard_sizing_reads_the_built_heap() {
    let planned = EngineSizing::for_store(&shape(ByteCount(256 * 1024 * 1024), 1));
    let built = reel::index::write_shard::WriteShard::new(&planned.shard);
    assert_eq!(planned.shard.heap_bytes(), built.heap_bytes());
}

// with every filter in the arena, the filter cache keeps one stripe and the row cache takes the rest, up to the row blocks on
// disk with the arena's margin, and the record cache takes what the row cache doesn't need
#[test]
fn idle_filter_room_goes_to_rows() {
    let stripe = 1_000;
    let small_rows = split_idle_room(10 * stripe, stripe, 2 * stripe, stripe);
    let large_rows = split_idle_room(10 * stripe, stripe, 2 * stripe, 100 * stripe);

    assert_eq!(small_rows, IdleSplit { filter_cache: stripe, to_rows: 0, to_records: 9 * stripe });
    assert_eq!(large_rows, IdleSplit { filter_cache: stripe, to_rows: 9 * stripe, to_records: 0 });
    let some_rows = split_idle_room(10 * stripe, stripe, 2 * stripe, 3 * stripe + stripe / 2);
    assert_eq!(some_rows.to_rows, 5 * stripe);
    assert_eq!(some_rows.filter_cache + some_rows.to_rows + some_rows.to_records, 10 * stripe);
}

// a filter cache the arena overflows into keeps its room, and nothing moves
#[test]
fn full_filter_room_stays() {
    let stripe = 1_000;
    assert_eq!(split_idle_room(stripe, stripe, 2 * stripe, 100 * stripe), IdleSplit { filter_cache: stripe, to_rows: 0, to_records: 0 });
}
// declared complete tables come out of the shares and raise the least budget
// most of the cache share to the write index
#[test]
fn declared_indexes_come_out_of_the_budget() {
    let budget = ByteCount(1024 * 1024 * 1024);
    let plain = shape(budget, 1);
    let declared = StoreShape { declared_bytes: ByteCount(200 * 1024 * 1024), ..plain };
    assert_eq!(declared.least_budget().0, plain.least_budget().0 + declared.declared_bytes.0);
    let (plain_sizing, declared_sizing) = (EngineSizing::for_store(&plain), EngineSizing::for_store(&declared));
    assert!(declared_sizing.cache_share < plain_sizing.cache_share);
    assert!(declared_sizing.record_share < plain_sizing.record_share);
}

// a sorted index asks for twice its keys with the headroom past them, at least the pages its image saved, and an empty one with
// no least asks for its part
#[test]
fn a_sorted_index_asks_by_its_keys() {
    use reel::engine::engine_sizing::{INDEX_GROWTH, INDEX_HEADROOM, IndexAsk, index_ask};
    use reel::index::sorted_keys::planned_index_bytes;

    assert_eq!(index_ask(16, 0, None, ByteCount(0)), IndexAsk::Part);
    let grown = planned_index_bytes(16, RowCount(1_000_000 * INDEX_GROWTH)).0 + INDEX_HEADROOM.0;
    assert_eq!(index_ask(16, 1_000_000, None, ByteCount(0)), IndexAsk::Bytes(ByteCount(grown)));
    assert_eq!(index_ask(16, 1_000_000, Some(RowCount(10_000_000)), ByteCount(0)), IndexAsk::Bytes(ByteCount(planned_index_bytes(16, RowCount(10_000_000)).0 + INDEX_HEADROOM.0)));
    assert_eq!(index_ask(16, 10, None, ByteCount(grown * 2)), IndexAsk::Bytes(ByteCount(grown * 2)));
}

// the indexes get their asks while they fit the cap, each the same fraction of its ask past it, and an empty one its part
#[test]
fn the_cap_is_shared_by_the_asks() {
    use reel::engine::engine_sizing::{IndexAsk, index_cap, share_indexes};

    assert_eq!(index_cap(400), 300);
    let asks = [IndexAsk::Bytes(ByteCount(100)), IndexAsk::Bytes(ByteCount(50))];
    assert_eq!(share_indexes(&asks, 300), vec![100, 50]);
    let asks = [IndexAsk::Bytes(ByteCount(400)), IndexAsk::Bytes(ByteCount(200))];
    assert_eq!(share_indexes(&asks, 300), vec![200, 100]);
    let asks = [IndexAsk::Part, IndexAsk::Bytes(ByteCount(50))];
    assert_eq!(share_indexes(&asks, 300), vec![150, 50]);
}

// a store of sorted columns at a budget, with no rows on disk
fn sorted_config(budget: u64, columns: u8) -> StoreConfig {
    StoreConfig {
        memory_budget: ByteCount(budget),
        threads: NO_WORKERS,
        volumes: vec![VolumeConfig::new("volume", vec![PathBuf::from("/disk/sized")], SyncPolicy::Never)],
        columns: (1..=columns)
            .map(|id| ColumnConfig {
                id: reel_api::column_id::ColumnId(id),
                name: format!("c{id}"),
                key_shape: KeyShape::Fixed { width: 16 },
                codec: Codec::None,
                volume: reel_api::units::VolumeId(0),
                complete_rows: None,
                sorted_rows: Some(RowCount(0)),
                least_value: None,
            })
            .collect(),
    }
}

// in a store whose columns are all sorted the spot share goes to the indexes first and ends at nothing, the write index keeps its
// share, and the caches and the index bytes add up to what the caches had
#[test]
fn an_all_sorted_store_gives_its_spot_share_to_the_indexes() {
    use reel::engine::reel_store::size_sorted_indexes;

    let io = reel_sim::simulated_io::SimulatedIo::new(reel_sim::seeded_random::Seed(1));
    for budget in [512 * 1024 * 1024, 2_048 * 1024 * 1024] {
        let config = sorted_config(budget, 1);
        let before = store_sizing(&config, false, &[None]).expect("sizing");
        let (after, sizes) = size_sorted_indexes(&io, &config, &before, &[]);
        let held: u64 = sizes.iter().flatten().map(|size| size.got.0).sum();
        assert_eq!(after.spot_share, ByteCount(0), "{budget} B");
        assert_eq!(after.segment_rows, before.segment_rows, "the write index keeps its share at {budget} B");
        let caches = |sizing: &EngineSizing| sizing.spot_share.0 + sizing.cache_share.0 + sizing.record_share.0;
        // the shares round down a byte or so each
        assert!(caches(&before).abs_diff(caches(&after) + held) <= 4, "{} against {} at {budget} B", caches(&before), caches(&after) + held);
        assert!(held > 0, "no index at {budget} B");
    }
}

// a grant under one pool's floor builds no index and charges the caches nothing, while a grant over it builds one at the bytes its
// pool holds
#[test]
fn a_grant_under_the_pool_floor_builds_no_index() {
    use reel::engine::reel_store::size_sorted_indexes;
    use reel::index::sorted_keys::planned_index_bytes;

    let io = reel_sim::simulated_io::SimulatedIo::new(reel_sim::seeded_random::Seed(2));
    let floor = planned_index_bytes(16, RowCount(1)).0;
    // the least budget a store of three sorted columns runs at
    let least = match store_sizing(&sorted_config(1 << 20, 3), false, &[None]) {
        Err(reel_api::store_error::StoreError::BudgetTooSmall { least, .. }) => least.0,
        other => panic!("a 1 MiB store sized: {other:?}"),
    };
    for (columns, is_built) in [(1, true), (3, false)] {
        let config = sorted_config(least, columns);
        let before = store_sizing(&config, false, &[None]).expect("sizing");
        let caches = before.spot_share.0 + before.cache_share.0 + before.record_share.0;
        let (after, sizes) = size_sorted_indexes(&io, &config, &before, &[]);
        let built: Vec<_> = sizes.iter().flatten().collect();
        assert_eq!(built.len(), usize::from(columns));
        for size in &built {
            assert_eq!(size.rows.0 > 0, is_built, "{columns} columns at {least} B, a floor of {floor} B: {size:?}");
            assert_eq!(size.got.0 > 0, is_built);
        }
        let held: u64 = built.iter().map(|size| size.got.0).sum();
        let after_caches = after.spot_share.0 + after.cache_share.0 + after.record_share.0;
        assert!(after_caches + held <= caches + 4, "{columns} columns took more than the caches had");
    }
}

// every size a large store gets, at the bench budgets with the ring and 8 or 32 workers (the box runs 32), held at the values the bench rows ran
// with: the fixed parts may move a small store's sizes and never these
#[test]
fn large_stores_keep_their_sizes() {
    use reel::PlainIo;
    use reel::engine::reel_store::{size_sorted_indexes, store_shape};
    use reel::engine::scan_state::{MOST_SCANS, planned_scan_bytes};
    const GIB: u64 = 1024 * 1024 * 1024;
    let mut found = Vec::new();
    for (budget, workers) in [8, 32].into_iter().flat_map(|workers| [2 * GIB, 4 * GIB, 8 * GIB].map(|budget| (budget, workers))) {
        let config = StoreConfig {
            memory_budget: ByteCount(budget),
            threads: ThreadCounts { maintenance_workers: Some(workers), ..ThreadCounts::default() },
            volumes: vec![VolumeConfig::new("kv", vec![PathBuf::from("/nonexistent/large")], SyncPolicy::Never)],
            columns: vec![ColumnConfig {
                id: ColumnId(1),
                name: "kv".to_string(),
                key_shape: KeyShape::Fixed { width: 16 },
                codec: Codec::None,
                volume: VolumeId(0),
                complete_rows: None,
                sorted_rows: Some(RowCount(10_000_000)),
                least_value: None,
            }],
        };
        let sizing = store_sizing(&config, true, &[None]).expect("sizing");
        let shape = store_shape(&config, true, &[None]).with_workers_settled();
        let scans = MOST_SCANS as u64 * planned_scan_bytes(shape.widest_keys, shape.open_slots());
        let (shared, sizes) = size_sorted_indexes(&PlainIo::new(), &config, &sizing, &[]);
        let index = sizes[1].expect("a sorted column");
        found.push([
            sizing.segment_bytes.0,
            sizing.segment_rows.0,
            sizing.spot_share.0,
            sizing.cache_share.0,
            sizing.record_share.0,
            sizing.summary_share.0,
            sizing.table_share.0,
            sizing.least_budget.0,
            shape.fixed_bytes().0,
            scans,
            shared.spot_share.0 + shared.cache_share.0 + shared.record_share.0,
            index.asked.0,
            index.got.0,
            index.rows.0,
        ]);
    }
    assert_eq!(found, LARGE_STORE_SIZES);
}

// segment bytes, segment rows, the spot, block, record, summary and table shares, the least budget, the fixed bytes, the scan
// room, the caches after the sorted index's share, and the index's asked bytes, got bytes and rows, at 2, 4 and 8 GiB with 8 workers, then 32
const LARGE_STORE_SIZES: [[u64; 14]; 6] = [
    [1073741824, 2403846, 291025766, 291025766, 291025766, 145512883, 145512883, 118691884, 108206124, 11433216, 145513010, 826335232, 727564288, 13047000],
    [1073741824, 4937850, 597809144, 597809144, 597809144, 298904572, 298904572, 118691884, 108206124, 11433216, 967092200, 826335232, 826335232, 14820750],
    [1073741824, 10005858, 1211375902, 1211375902, 1211375902, 605687951, 605687951, 118691884, 108206124, 11433216, 2807792474, 826335232, 826335232, 14820750],
    [1073741824, 2237645, 270904372, 270904372, 270904372, 135452186, 135452186, 259541644, 249055884, 11433216, 135455900, 826335232, 677257216, 12143550],
    [1073741824, 4771649, 577687750, 577687750, 577687750, 288843875, 288843875, 259541644, 249055884, 11433216, 906728018, 826335232, 826335232, 14820750],
    [1073741824, 9839656, 1191254508, 1191254508, 1191254508, 595627254, 595627254, 259541644, 249055884, 11433216, 2747428292, 826335232, 826335232, 14820750],
];
