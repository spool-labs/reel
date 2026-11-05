//! How one memory budget splits across the engine

use reel_api::units::{ByteCount, RowCount};

use crate::engine::open_segment::RECORD_WINDOW_BYTES;
use crate::index::entry_slab::{PAGE_BYTES, PageCount};
use crate::index::node_pool::{NODE_FANOUT, NodeCount};
use crate::index::open_slots::MAX_OPEN_SLOTS;
use crate::index::write_index::SHARD_COUNT;
use crate::index::write_shard::ShardSizing;
use crate::record::codec::CHUNK_BYTES;
use crate::record::record_head::SMALL_HEAD_BYTES;
use crate::segment::damage_list::{MOST_DAMAGED_SPANS, SPAN_KEY_BYTES};
use crate::segment::range_covers::{COVER_RECORD_BYTES, COVER_SLOT_BYTES, MOST_RANGE_COVERS};
use crate::segment::segment_tail::MOST_SEGMENT_BYTES;

/// A segment's target size on a store with a full budget, and a smaller budget targets half of itself
pub const SEGMENT_TARGET: ByteCount = ByteCount(1024 * 1024 * 1024);
/// A budget this large or more gets the full segment target
pub const FULL_BUDGET: ByteCount = ByteCount(512 * 1024 * 1024);
/// Open segments a root can have
pub const OPEN_PER_ROOT: usize = 4;
/// Open slots each open segment can need: its own, and one while it seals
pub const SLOTS_PER_OPEN: usize = 2;
/// The write index gets one part in this many of what the fixed parts leave
pub const WRITE_INDEX_DIVISOR: u64 = 2;
/// The spot cache gets one part in this many of what the fixed parts leave
pub const SPOT_DIVISOR: u64 = 8;
/// The block cache gets one part in this many of what the fixed parts leave
pub const BLOCK_CACHE_DIVISOR: u64 = 8;
/// The summaries get one part in this many of what the fixed parts leave, rows and the save buffer together. A row costs
/// about 100 B a column and four bytes a row block, about 0.003 B a key at 1,400 rows a block, so 1B keys take 2.8 MB of
/// it. A row that doesn't fit is dropped: its segment is never walked or counted against, which only understates
pub const SUMMARY_DIVISOR: u64 = 16;
/// The segment tables and column directories get one part in this many of what the fixed parts leave
pub const TABLE_DIVISOR: u64 = 16;
/// Bytes the table plans for a segment before its columns
pub const PLANNED_SEGMENT_BYTES: u64 = 512;
/// Bytes the table and directories plan for a column of a segment
pub const PLANNED_COLUMN_BYTES: u64 = 240;
/// Bytes the table plans for a line of a block index
pub const PLANNED_LINE_BYTES: u64 = 32;
/// The least a budget has to leave after the fixed parts
pub const LEAST_SHARED_BYTES: ByteCount = ByteCount(8 * 1024 * 1024);
/// Bytes a read buffer pair is planned at: a record reader and a footer reader
pub const READ_BUFFER_BYTES: u64 = 512 * 1024;
/// Bytes a record encoder is planned at, with no share of the compress pool
pub const ENCODER_BYTES: u64 = 256 * 1024;
/// Bytes a seal job's own buffers are planned at
pub const SEAL_JOB_BYTES: u64 = 3 * 1024 * 1024;
/// Bytes a dead-byte walk's own buffers are planned at
pub const WALK_JOB_BYTES: u64 = 5 * 256 * 1024;
/// Most workers a volume's walk has, which sets how far the walk can stay behind the seal ceiling
pub const MOST_WALK_WORKERS: usize = 2;
/// A store gets one walk worker a volume for each this much budget, up to the most
pub const WALK_WORKER_BUDGET: ByteCount = ByteCount(64 * 1024 * 1024);
/// Bytes a compaction job's own buffers are planned at
pub const COMPACT_JOB_BYTES: u64 = 5 * 1024 * 1024;
/// Bytes a job's stack is planned at
pub const JOB_STACK_PLAN_BYTES: u64 = 1024 * 1024;
/// Jobs a store runs beside one seal job a volume: row write-out, reservations and the planner
pub const SHARED_JOBS: u64 = 3;
/// Bytes a row of the write index really takes, node, page and table share included, with a long batch's length of it
pub const PLANNED_ROW_BYTES: u64 = 227;
/// A volume seals at this share of what the index holds, in eighths, to keep room for writers in flight. With one
/// lz4 column and 64 B keys that gives these rows a segment: 38,906 at 128 MiB, 131,292 at 192 MiB, 593,220 at 512 MiB and
/// 5,766,811 at 4 GiB. A segment seals at its byte target or at this limit, whichever comes first, so small
/// records seal smaller and reach their size through merges, whose outputs the write index doesn't bound
/// A volume holds its open segment and one spare at full size even when its sealed data is small: the reserve is twice the
/// segment size, 128 MiB at a 128 MiB budget, 192 MiB at 192 MiB, and 2 GiB from the default 512 MiB up. The space gate adds
/// this to the live bytes
pub const SEAL_ROWS_EIGHTHS: u64 = 5;
/// The most stored bytes of a value on an lz4 column, which is also the compress pool, cut into shares
pub const LZ4_STORED_LIMIT: ByteCount = ByteCount(64 * 1024 * 1024);
/// Bytes of one row group buffer
pub const GROUP_BYTES: ByteCount = ByteCount(256 * 1024);
/// A store with an lz4 column needs at least this budget, so its compress pool fits inside
pub const LEAST_LZ4_BUDGET: ByteCount = ByteCount(2 * LZ4_STORED_LIMIT.0);
/// The share of the block cache filters get, in quarters, since every get probes them
pub const FILTER_CACHE_QUARTERS: u64 = 3;
/// Bytes of a filter cache slot, which holds the filter of a full block of typical rows
pub const FILTER_SLOT_BYTES: usize = 4 * 1024;
/// Read buffers a store keeps
pub const READ_BUFFERS: usize = 8;
/// Bytes of the buffer a batch gathers its records in, which holds the largest one-chunk record
pub const GATHER_BYTES: usize = CHUNK_BYTES + SMALL_HEAD_BYTES;
/// Write buffers a store keeps
pub const WRITE_BUFFERS: usize = 8;

/// What a store is made of, which fixes the parts a budget can't shrink
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StoreShape {
    pub budget: ByteCount,
    pub roots: usize,
    pub volumes: usize,
    pub has_lz4: bool,
    pub widest_keys: u64,
}

/// What a budget buys
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EngineSizing {
    pub segment_bytes: ByteCount,
    pub segment_rows: RowCount,
    pub shard: ShardSizing,
    pub spot_share: ByteCount,
    pub cache_share: ByteCount,
    pub summary_share: ByteCount,
    pub table_share: ByteCount,
    pub lz4_stored_limit: ByteCount,
    pub walk_workers: usize,
}

impl StoreShape {
    /// Count the walk workers each volume gets
    pub fn walk_workers(&self) -> usize {
        ((self.budget.0 / WALK_WORKER_BUDGET.0) as usize).min(MOST_WALK_WORKERS)
    }

    /// Count the open slots the roots can use
    pub fn open_slots(&self) -> usize {
        (self.roots * OPEN_PER_ROOT * SLOTS_PER_OPEN).min(MAX_OPEN_SLOTS)
    }

    /// Add up what a store holds whatever its budget: buffers, pools, stacks and the spans a summary must keep
    pub fn fixed_bytes(&self) -> ByteCount {
        let volumes = self.volumes as u64;
        let queues = (self.open_slots() as u64 * 2 + WRITE_BUFFERS as u64) * GROUP_BYTES.0;
        let gathers = WRITE_BUFFERS as u64 * GATHER_BYTES as u64;
        let windows = self.open_slots() as u64 * 3 * RECORD_WINDOW_BYTES as u64;
        let reads = READ_BUFFERS as u64 * READ_BUFFER_BYTES;
        let large = if self.has_lz4 { LZ4_STORED_LIMIT.0 } else { 0 };
        let encoders = WRITE_BUFFERS as u64 * ENCODER_BYTES + large;
        let spans = 4 * MOST_DAMAGED_SPANS as u64 * (SPAN_KEY_BYTES as u64 * 2);
        let workers = volumes * self.walk_workers() as u64;
        let stacks = (SHARED_JOBS + volumes + workers) * JOB_STACK_PLAN_BYTES;
        // the pool holds both keys of every cover, and a seal job has room to write them all to a footer
        let cover_keys = 2 * MOST_RANGE_COVERS as u64 * self.widest_keys;
        let cover_fixed = (COVER_SLOT_BYTES + COVER_RECORD_BYTES) as u64 * MOST_RANGE_COVERS as u64;
        let covers = 2 * cover_keys + volumes * cover_fixed;
        let jobs = SEAL_JOB_BYTES + WALK_JOB_BYTES + COMPACT_JOB_BYTES;
        ByteCount(queues + gathers + windows + reads + encoders + covers + volumes * (jobs + spans) + workers * WALK_JOB_BYTES + stacks)
    }

    /// Read the smallest budget this store can run in
    pub fn least_budget(&self) -> ByteCount {
        ByteCount(self.fixed_bytes().0 + LEAST_SHARED_BYTES.0)
    }
}

impl EngineSizing {
    /// Share out what a budget leaves after the fixed parts
    pub fn for_store(shape: &StoreShape) -> Self {
        let open_slots = shape.open_slots();
        let budget = shape.budget;
        let shared = budget.0.saturating_sub(shape.fixed_bytes().0);
        let index_share = shared / WRITE_INDEX_DIVISOR;
        let rows_total = index_share / PLANNED_ROW_BYTES;
        let rows_per_shard = (rows_total / SHARD_COUNT as u64).max(64);

        let least_pages = (open_slots + 1) as u64;
        let page_rows = (PAGE_BYTES / 64) as u64;
        let pages = (rows_per_shard / page_rows + 1).max(least_pages) * 2;
        let nodes = (rows_per_shard / (NODE_FANOUT as u64 / 2) + 1) * 2 + 64;

        let segment_bytes = if budget >= FULL_BUDGET {
            SEGMENT_TARGET
        } else {
            ByteCount(budget.0 / 2)
        };
        Self {
            segment_bytes: ByteCount(segment_bytes.0.min(MOST_SEGMENT_BYTES.0 / 2)),
            segment_rows: RowCount(rows_total * SEAL_ROWS_EIGHTHS / 8),
            shard: ShardSizing {
                pages: PageCount(pages as u32),
                nodes: NodeCount(nodes as u32),
                table_slots: (rows_per_shard as usize * 2).next_power_of_two(),
                open_slots,
            },
            spot_share: ByteCount(shared / SPOT_DIVISOR),
            cache_share: ByteCount(shared / BLOCK_CACHE_DIVISOR),
            summary_share: ByteCount(shared / SUMMARY_DIVISOR),
            table_share: ByteCount(shared / TABLE_DIVISOR),
            lz4_stored_limit: LZ4_STORED_LIMIT,
            walk_workers: shape.walk_workers(),
        }
    }
}
