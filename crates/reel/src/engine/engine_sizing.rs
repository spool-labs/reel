//! How one memory budget splits across the engine

use reel_api::units::{ByteCount, RowCount};

use crate::index::entry_slab::{PAGE_BYTES, PageCount};
use crate::index::node_pool::{NODE_FANOUT, NodeCount};
use crate::index::open_slots::MAX_OPEN_SLOTS;
use crate::index::write_index::SHARD_COUNT;
use crate::index::write_shard::ShardSizing;
use crate::segment::segment_tail::MOST_SEGMENT_BYTES;

/// A segment's target size on a store with a full budget
pub const SEGMENT_TARGET: ByteCount = ByteCount(1024 * 1024 * 1024);
/// A budget this large or more gets the full segment target
pub const FULL_BUDGET: ByteCount = ByteCount(512 * 1024 * 1024);
/// Open segments a root can have
pub const OPEN_PER_ROOT: usize = 4;
/// Open slots each open segment can need: its own, and one while it seals
pub const SLOTS_PER_OPEN: usize = 2;
/// The share of the budget the write index gets, as a divisor
pub const WRITE_INDEX_DIVISOR: u64 = 4;
/// The share of the budget the spot cache gets, as a divisor
pub const SPOT_DIVISOR: u64 = 4;
/// The share of the budget the block cache gets, as a divisor
pub const BLOCK_CACHE_DIVISOR: u64 = 8;
/// Bytes a row of the write index is planned at, node and table share included
pub const PLANNED_ROW_BYTES: u64 = 112;
/// A volume seals at this share of what the index holds, in eighths, to keep room for writers in flight
pub const SEAL_ROWS_EIGHTHS: u64 = 5;
/// The most stored bytes of a value on an lz4 column
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
/// Write buffers a store keeps
pub const WRITE_BUFFERS: usize = 4;

/// What a budget buys
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EngineSizing {
    pub segment_bytes: ByteCount,
    pub segment_rows: RowCount,
    pub shard: ShardSizing,
    pub spot_share: ByteCount,
    pub cache_share: ByteCount,
    pub lz4_stored_limit: ByteCount,
}

impl EngineSizing {
    /// Split a budget for a store with this many roots
    pub fn for_budget(budget: ByteCount, roots: usize) -> Self {
        let open_slots = (roots * OPEN_PER_ROOT * SLOTS_PER_OPEN).min(MAX_OPEN_SLOTS);
        let index_share = budget.0 / WRITE_INDEX_DIVISOR;
        let rows_total = index_share / PLANNED_ROW_BYTES;
        let rows_per_shard = (rows_total / SHARD_COUNT as u64).max(64);

        let least_pages = (open_slots + 1) as u64;
        let page_rows = (PAGE_BYTES / 64) as u64;
        let pages = (rows_per_shard / page_rows + 1).max(least_pages) * 2;
        let nodes = (rows_per_shard / (NODE_FANOUT as u64 / 2) + 1) * 2 + 64;

        let segment_bytes = if budget >= FULL_BUDGET {
            SEGMENT_TARGET
        } else {
            ByteCount(budget.0 * 2)
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
            spot_share: ByteCount(budget.0 / SPOT_DIVISOR),
            cache_share: ByteCount(budget.0 / BLOCK_CACHE_DIVISOR),
            lz4_stored_limit: LZ4_STORED_LIMIT,
        }
    }
}
