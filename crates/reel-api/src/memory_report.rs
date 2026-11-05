//! Heap held by each structure, with no profiler attached

use crate::units::ByteCount;

/// Every named part of the engine's heap
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct MemoryReport {
    pub budget: ByteCount,
    pub write_index: ByteCount,
    pub spot_cache: ByteCount,
    pub block_cache: ByteCount,
    pub block_indexes: ByteCount,
    pub segment_table: ByteCount,
    pub summary: ByteCount,
    pub range_deletes: ByteCount,
    pub key_runs: ByteCount,
    pub read_buffers: ByteCount,
    pub copy_buffers: ByteCount,
    pub compress_buffers: ByteCount,
    pub queues: ByteCount,
    pub footer_space: ByteCount,
    pub thread_stacks: ByteCount,
}
