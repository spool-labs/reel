//! Heap held by each structure, with no profiler attached

use crate::units::{ByteCount, Nanoseconds, SegmentCount};

/// Every named part of the engine's heap
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct MemoryReport {
    pub budget: ByteCount,
    pub write_index: ByteCount,
    pub spot_cache: ByteCount,
    pub complete_tables: ByteCount,
    /// Bytes the sorted indexes hold, on top of the budget as complete tables are
    pub sorted_indexes: ByteCount,
    pub block_cache: ByteCount,
    pub record_cache: ByteCount,
    /// Bytes the dead bits' pools hold, one bit for each charged row
    pub dead_bits: ByteCount,
    pub block_indexes: ByteCount,
    pub segment_table: ByteCount,
    pub summary: ByteCount,
    pub damaged_spans: ByteCount,
    pub range_deletes: ByteCount,
    pub read_buffers: ByteCount,
    pub scans: ByteCount,
    pub copy_buffers: ByteCount,
    pub compress_buffers: ByteCount,
    pub queues: ByteCount,
    pub footer_space: ByteCount,
    pub compaction: ByteCount,
    pub thread_stacks: ByteCount,
    /// Open segment slots the store sized at open, each with its queues and windows in the parts above
    pub open_slots: usize,
    /// Workers each volume runs beside its planner, which the budget may have set below the default
    pub maintenance_workers: usize,
    /// The store was opened read-only: it runs no workers and no write side, and its write index holds the open rows its last
    /// load found
    pub is_read_only: bool,
    /// Read threads the store runs for the async calls, none when a caller uses only the blocking ones
    pub read_threads: usize,
    /// Async reads the store holds in flight at once
    pub read_slots: usize,
    /// The least budget this store opens in with the workers and the ring it runs, a figure with no target on it
    pub least_budget: ByteCount,
    /// The fixed room the plan keeps for heap no part names: the rounding, and room a foreground job for its allocator's state
    pub room: ByteCount,
    /// How long the last open took, from the root locks to every job running
    pub open_time: Nanoseconds,
    /// The part of that open spent building and touching the pools, the jobs' included
    pub prefault_time: Nanoseconds,
    /// The process's anonymous memory the kernel backed with huge pages once the store had opened, which its arenas ask for
    pub huge_pages: ByteCount,
    /// The filter arena holds every filter it can, and gets search the footers' own filters until it does
    pub is_arena_full: bool,
    /// Segments with filters the arena lacks, which a full arena left out for want of room
    pub arena_segments_left_out: SegmentCount,
    /// Bytes the filter arena's extents take, its whole room, and the rows of the segments whose filters it holds
    pub arena_used: ByteCount,
    pub arena_room: ByteCount,
    pub arena_rows: u64,
    /// Columns of those segments
    pub arena_columns_left_out: usize,
    /// The line the plan keeps for the segment entries of volumes with a known disk size, apart from the table's share
    pub entry_line: ByteCount,
    /// The size of the distinct filesystems under those volumes' roots
    pub disk_bytes: ByteCount,
    /// The disk bytes the entries cover, short of the disks when the line met its cap, and seals stop past it
    pub disk_covered: ByteCount,
    /// Bytes the pools that grow with their data hold in memory now, which they map apart from the allocator
    pub pool_committed: ByteCount,
    /// Bytes those pools can grow to, their shares of the budget
    pub pool_shares: ByteCount,
    /// Pools the host had no room to grow, which run as full pools until a step fits
    pub pools_short: usize,
}

impl MemoryReport {
    /// Add up every named part of the heap, stacks apart since they aren't heap
    pub fn heap_total(&self) -> ByteCount {
        ByteCount(
            self.write_index.0
                + self.spot_cache.0
                + self.complete_tables.0
                + self.sorted_indexes.0
                + self.block_cache.0
                + self.record_cache.0 + self.dead_bits.0
                + self.block_indexes.0
                + self.segment_table.0
                + self.summary.0
                + self.damaged_spans.0
                + self.range_deletes.0
                + self.read_buffers.0
                + self.scans.0
                + self.copy_buffers.0
                + self.compress_buffers.0
                + self.queues.0
                + self.footer_space.0
                + self.compaction.0,
        )
    }
}
