//! Heap held by each structure, with no profiler attached

use crate::units::{ByteCount, SegmentCount};

/// Every named part of the engine's heap
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct MemoryReport {
    pub budget: ByteCount,
    pub write_index: ByteCount,
    pub spot_cache: ByteCount,
    pub complete_tables: ByteCount,
    pub block_cache: ByteCount,
    pub block_indexes: ByteCount,
    pub segment_table: ByteCount,
    pub summary: ByteCount,
    pub damaged_spans: ByteCount,
    pub range_deletes: ByteCount,
    pub key_runs: ByteCount,
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
    /// The filter arena holds every filter it can, and gets search the footers' own filters until it does
    pub is_arena_full: bool,
    /// Segments with filters the arena lacks, which a full arena left out for want of room
    pub arena_segments_left_out: SegmentCount,
    /// Columns of those segments
    pub arena_columns_left_out: usize,
}

impl MemoryReport {
    /// Add up every named part of the heap, stacks apart since they aren't heap
    pub fn heap_total(&self) -> ByteCount {
        ByteCount(
            self.write_index.0
                + self.spot_cache.0
                + self.complete_tables.0
                + self.block_cache.0
                + self.block_indexes.0
                + self.segment_table.0
                + self.summary.0
                + self.damaged_spans.0
                + self.range_deletes.0
                + self.key_runs.0
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
