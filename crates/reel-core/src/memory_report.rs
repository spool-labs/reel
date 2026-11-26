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
    /// Bytes of the compress shares' pages the ladder gave back, out of compress_buffers and held until they come back
    pub compress_given_back: ByteCount,
    /// Bytes the lists that grow with their segments hold in memory now, which they map apart from the allocator. The block indexes
    /// and the segment table count them too
    pub list_committed: ByteCount,
    /// Bytes of those pools' shares of the budget, which a sorted index's pool can grow past while the target has room
    pub pool_shares: ByteCount,
    /// Pools that asked for address space past their share and reserved the share alone, because the process passed its ceiling
    /// or the host refused the map. A sorted index among them marks once it outgrows its plan, and comes back at the next open
    pub pools_held_to_share: u64,
    /// The budget the config asked for. Over the budget in force when the host had less free at open, which the store planned from
    pub budget_asked: ByteCount,
    /// Pools the host had no room to grow, which run as full pools until a step fits
    pub pools_short: usize,
    /// Bytes the pools have given back to the kernel since open
    pub pool_given_back: ByteCount,
    /// The memory the store holds itself to now, and what it holds: the heap outside its pools, measured at open, and the pools
    pub memory_target: ByteCount,
    pub memory_held: ByteCount,
    /// The host has less than half its margin free, and the store gives back what the host lacks
    pub is_under_pressure: bool,
    /// The ladder could not bring the store down to its target: the caches are spent, and what is left holds data
    pub is_target_short: bool,
    /// Where the store learns of memory pressure, chosen at open
    pub pressure_source: PressureSource,
    /// Pressure events the kernel sent through a trigger since open
    pub pressure_events: u64,
    /// Ladder steps that found the store under pressure since open, from the kernel or the host. The flag above holds only for
    /// the step that cut, so a scrape counts these
    pub pressed_steps: u64,
    /// Sorted indexes dropped whole under pressure since open, whose reads ask the footers until a refill
    pub sorted_drops: u64,
    /// Dropped sorted indexes a refill brought back since open
    pub sorted_refills: u64,
    /// Seals and merges whose table or directory lines couldn't come into memory, since open, each retried later
    pub populate_failures: u64,
    /// Spot index shards given back, which read as missing until they come back. 0 when the spot index is whole
    pub spot_absent_shards: u64,
    /// Block cache shards given back, filter, row and complete caches together. 0 when they are whole
    pub block_absent_shards: u64,
    /// Shards of the spot index and the block caches the ladder gave back since open
    pub cache_shards_given: u64,
    /// The bytes the spot, block and record caches may hold, which a rebalance lowers by whole shards to their floors
    pub cache_ceilings: [ByteCount; 3],
    /// The bytes the spot, block and record caches hold now, under their ceilings
    pub cache_present: [ByteCount; 3],
    /// Bytes the sorted indexes may grow past the shares open gave them, which rebalances moved off the caches
    pub index_past_share: ByteCount,
    /// Times a rebalance moved room from the caches to a sorted index since open
    pub rebalances: u64,
    /// Sorted indexes past their room that the open-time rule gives no more, since the caches are at their floor or the cap is
    /// shared out
    pub indexes_at_cap: u64,
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

/// Where a store learns of memory pressure, the best the host offers, chosen at open
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum PressureSource {
    /// A trigger on the control group's memory.pressure, which the kernel fires once tasks stall past a mark
    Trigger,
    /// A read of the control group's memory.pressure each step, where the store may not write a trigger
    Read,
    /// The host's free memory, read each step, where the kernel keeps no pressure for the group
    #[default]
    Host,
}
