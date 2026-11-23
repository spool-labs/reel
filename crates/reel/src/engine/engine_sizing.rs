//! How one memory budget splits across the engine

use reel_core::units::{ByteCount, RowCount};

use crate::config::{MOST_VOLUME_SLOTS, ThreadCounts, default_workers};

use crate::engine::read_pool::{READ_SLOTS, SLOT_KEY_BYTES, SLOT_KEYS};
use crate::engine::seal_job::MOST_SEGMENT_BLOCKS;
use crate::engine::scan_state::{MOST_SCANS, planned_scan_bytes};
use crate::index::entry_slab::{PAGE_BYTES, PageCount};
use crate::index::sorted_keys::planned_index_bytes;
use crate::index::node_pool::{NODE_FANOUT, NodeCount};
use crate::index::open_slots::MAX_OPEN_SLOTS;
use crate::io::ring::{RING_READ_BYTES, RING_READS, RING_SLOT_BYTES, RING_SLOTS};
use crate::index::write_index::SHARD_COUNT;
use crate::index::write_shard::ShardSizing;
use crate::record::codec::CHUNK_BYTES;
use crate::record::record_head::SMALL_HEAD_BYTES;
use crate::segment::damage_list::{MOST_DAMAGED_SPANS, SPAN_KEY_BYTES};
use crate::segment::range_covers::{COVER_RECORD_BYTES, COVER_SLOT_BYTES, MOST_RANGE_COVERS};
use crate::segment::column_directory::Candidate;
use crate::segment::footer_reader::CANDIDATE_PAGE;
use crate::segment::row_block::{BLOCK_TRAILER_BYTES, COMPLETE_BLOCK_BYTES, ROW_BLOCK_BYTES, ROW_ROOM_NUMBERS};
use crate::segment::segment_table::TABLE_SHARDS;
use crate::segment::segment_tail::{BlockCount, MAX_COLUMNS, MAX_TAIL_BYTES, MOST_SEGMENT_BYTES};

use crate::config::{FORMAT_ROW_FLOOR, StoreConfig};
use crate::segment::summary_orders::ORDER_BYTES;

/// A segment's target size on a store with a full budget, and a smaller budget targets half of itself
pub const SEGMENT_TARGET: ByteCount = ByteCount(1024 * 1024 * 1024);
/// A budget this large or more gets the full segment target
pub const FULL_BUDGET: ByteCount = ByteCount(512 * 1024 * 1024);
/// Open segments a root can have
pub const OPEN_PER_ROOT: usize = 4;
/// Open slots each open segment can need: its own, and one while it seals
pub const SLOTS_PER_OPEN: usize = 2;
/// The write index's sixteenths of what the fixed parts and the room leave
pub const WRITE_INDEX_SIXTEENTHS: u64 = 6;
/// The write index's sixteenths the sorted indexes' leaves keep values in on a full budget, so a scan or get of a held key
/// reads no record. A smaller budget keeps them in the write index, and a store with no sorted column gives them to the record cache
///
/// Based on calculations: a held value costs about its length rounded to 8 B plus (4 x leaf capacity + 2,048) / entries a leaf,
/// as value_pages.rs works out, so a column is held whole once this share of the shared budget is that cost times its keys. A
/// larger share holds more and leaves the write index fewer rows, so it seals sooner
pub const HELD_VALUE_SIXTEENTHS: u64 = 2;
/// The spot cache's sixteenths of what the fixed parts and the room leave
pub const SPOT_SIXTEENTHS: u64 = 2;
/// The block caches' sixteenths of what the fixed parts and the room leave
pub const BLOCK_CACHE_SIXTEENTHS: u64 = 2;
/// The record cache's sixteenths of what the fixed parts and the room leave, so a hot get reads no file
pub const RECORD_CACHE_SIXTEENTHS: u64 = 2;
/// Times the filters of the rows on disk at open the arena may hold, room for the store to grow. A starting value, tuned at the end
pub const ARENA_GROWTH: u64 = 2;
/// Times the keys a sorted index held at open it asks room for, so the store grows until the next open with no mark
pub const INDEX_GROWTH: u64 = 2;
/// Times the open rows a read-only load found its write index holds, room for a writer that keeps adding until the next refresh
pub const OPEN_ROWS_GROWTH: u64 = 2;
/// The most headroom one sorted index asks for past its grown keys
pub const INDEX_HEADROOM: ByteCount = ByteCount(256 * 1024 * 1024);
/// Quarters of the cap a store keeps as headroom for its sorted indexes, shared evenly among them, so a small index never sits at
/// twice a small count
pub const HEADROOM_QUARTERS: u64 = 1;
/// Quarters of the spot, block and record caches they keep however much the sorted indexes ask, and the rest is the indexes' cap
pub const CACHE_FLOOR_QUARTERS: u64 = 1;
/// Rows a fresh store holds dead bits and filters for before its segments start going without. A missing bit asks and a
/// missing filter reads through the cache, so past it every answer stays right and takes the slow path, which the report
/// shows. A starting value, tuned at the end
pub const FLOOR_ROWS: u64 = 16_000_000;
/// The summaries' sixteenths of what the fixed parts and the room leave, rows and the save buffer together. A row costs about
/// 100 B a column and four bytes a row block, about 0.003 B a key at 1,400 rows a block, so 1B keys take 2.8 MB of it. A row that
/// doesn't fit is dropped: its segment is never walked or counted against, which only understates
pub const SUMMARY_SIXTEENTHS: u64 = 1;
/// The segment tables' and column directories' sixteenths of what the fixed parts and the room leave
pub const TABLE_SIXTEENTHS: u64 = 1;
/// Sixteenths every share takes together
pub const SHARED_SIXTEENTHS: u64 = WRITE_INDEX_SIXTEENTHS + SPOT_SIXTEENTHS + BLOCK_CACHE_SIXTEENTHS + RECORD_CACHE_SIXTEENTHS + SUMMARY_SIXTEENTHS + TABLE_SIXTEENTHS;
/// Bytes a segment's entry plans for before its columns: its table entry, slot and pin, its places in the table's and the
/// summary's number maps at their worst rounding, the 16 B the planner's covered list holds for it, and its place in the
/// summary's orders
pub const PLANNED_SEGMENT_BYTES: u64 = 280 + ORDER_BYTES + PLANNED_SPARE_BYTES;
/// Bytes a column of a segment plans for: its tail in the table, and an entry in each list of its directory
pub const PLANNED_COLUMN_BYTES: u64 = 536;
/// Bytes a segment plans for in its volume's spare directory, which every column of the volume builds its changes in
pub const PLANNED_SPARE_BYTES: u64 = 416;
/// Entries a volume plans for each segment its disks hold at the segment size, since small records seal smaller
pub const ENTRY_MARGIN: u64 = 2;
/// Entries a table shard holds at the least, whatever the disks
pub const LEAST_SHARD_ENTRIES: usize = 8;
/// The entries past each table's floor take at most one part in this many of what the budget has past its least. Past that the
/// entries cover less than the disk, and a seal stops once the segments pass them
pub const ENTRY_LINE_DIVISOR: u64 = 4;
/// Bytes the table plans for a line of a block index
pub const PLANNED_LINE_BYTES: u64 = 32;
/// The least a budget has to leave after the fixed parts
pub const LEAST_SHARED_BYTES: ByteCount = ByteCount(8 * 1024 * 1024);
/// Room for the index's power-of-two table and the caches' least sizes, which run past their shares at a small budget
pub const ROUNDING_ROOM: u64 = 1024 * 1024;
/// Heap a foreground job's thread holds past what the plan names, its allocator's own state, which the room keeps for each
pub const JOB_ROOM_BYTES: u64 = 128 * 1024;
/// Bytes a read buffer pair is planned at: a record reader and a footer reader
pub const READ_BUFFER_BYTES: u64 = 512 * 1024;
/// Bytes a maintenance thread's footer reader is planned at: a tail, a block, a filter, a key of a block's length and a page of
/// candidates
pub const JOB_READER_BYTES: u64 = (MAX_TAIL_BYTES + 4 * ROW_BLOCK_BYTES + CANDIDATE_PAGE * size_of::<Candidate>()) as u64;
/// Bytes a record encoder is planned at, with no share of the compress pool
pub const ENCODER_BYTES: u64 = 256 * 1024;
/// Bytes a seal job's own buffers are planned at
pub const SEAL_JOB_BYTES: u64 = 13 * 256 * 1024;
/// Bytes a dead-byte walk's own buffers are planned at
pub const WALK_JOB_BYTES: u64 = 9 * 256 * 1024;
/// Bytes a volume's compaction job holds from open: its block, keys and settled rows, the planner lane's pick key and cursor. A
/// rewrite's lists come from the region it maps at its start, outside the budget
pub const COMPACT_JOB_BYTES: u64 = 128 * 1024 + CURSOR_BYTES;
/// Bytes a worker's rewrite lane holds from open: its pick key, its inputs' places and its cursor
pub const LANE_JOB_BYTES: u64 = 4 * 1024 + CURSOR_BYTES;
/// Bytes a key cursor holds, in every walk counter and rewrite lane
pub const CURSOR_BYTES: u64 = 256 * 1024;
/// Bytes a job's stack is planned at
pub const JOB_STACK_PLAN_BYTES: u64 = 1024 * 1024;
/// Jobs a store runs beside one seal job a volume: row write-out, reservations and the planner
pub const SHARED_JOBS: u64 = 3;
/// Bytes a row of the write index really takes, node, page and table share included, with a long batch's length of it
pub const PLANNED_ROW_BYTES: u64 = 227;
/// A volume seals at this share of what the index holds, in eighths, to keep room for writers in flight. With one
/// lz4 column and 64 B keys that gives these rows a segment: 31,190 at 128 MiB, 136,774 at 192 MiB, 664,691 at 512 MiB and
/// 6,577,366 at 4 GiB. A segment seals at its byte target or at this limit, whichever comes first, so small
/// records seal smaller and reach their size through merges, whose outputs the write index doesn't bound
/// A volume holds its open segment and one spare at full size even when its sealed data is small: the reserve is twice the
/// segment size, 128 MiB at a 128 MiB budget, 192 MiB at 192 MiB, and 2 GiB from the default 512 MiB up. The space gate adds
/// this to the live bytes
/// A seal starts at this many eighths of the point the measured rates give, since a seal slows under the load it runs beside
pub const SEAL_POINT_EIGHTHS: u64 = 7;
/// A seal never starts below one part in this many of what the index holds, however far the rates say ingest outruns it
pub const LEAST_SEAL_DIVISOR: u64 = 4;
/// Nanoseconds in a second, for a rate
pub const SECOND: u64 = 1_000_000_000;
/// Eighths of the rows the index plans for that a segment may reach
pub const SEAL_ROWS_EIGHTHS: u64 = 5;
/// The write index holds this many segments' worth of rows past the seal's margin when sized for a store's mean record, so the
/// seal point's floor of a quarter of the index stays past one segment and the byte target seals first
pub const INDEX_ROWS_MARGIN: u64 = 3;
/// An index sized for a store's mean record keeps at least one part in this many of the planned rows
pub const LEAST_INDEX_DIVISOR: u64 = 4;
/// A seal starts with room for this part of a second of its own rate, or the line a ration draws on barely moves
pub const LEAST_ROOM_DIVISOR: u128 = 10;
/// The eighths of a segment's row block lines a complete column can fill. The rest stay free for other columns' blocks
pub const SEAL_LINE_EIGHTHS: u64 = 7;
/// The most bytes a row's numbers can take in a block: kind and shared length, suffix length, place, offset and length
pub const WORST_ROW_NUMBERS: usize = 24;
/// The bytes a block keeps for its restart points
pub const RESTART_ROOM: usize = COMPLETE_BLOCK_BYTES / 32;
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
/// Eighths of the filters' share that stay resident, and the rest cache what the arena has no room for
/// Read buffers a store with the full budget keeps
pub const READ_BUFFERS: usize = 8;
/// Read buffers a store keeps for each maintenance worker, one a CPU, so callers past the CPUs still each find one
pub const READERS_PER_WORKER: usize = 4;

/// Bytes of the buffer a batch gathers its records in, which holds the largest one-chunk record
pub const GATHER_BYTES: usize = CHUNK_BYTES + SMALL_HEAD_BYTES;
/// Write buffers a store with the full budget keeps
pub const WRITE_BUFFERS: usize = 8;

/// The budget a store needs to run every scan, read slot, read buffer and write buffer the constants allow. A smaller one runs
/// a share of each in step with its budget, down to the least counts
pub const FULL_COUNTS_BUDGET: ByteCount = ByteCount(1024 * 1024 * 1024);
/// The fewest scans a store runs at once
pub const LEAST_SCANS: usize = 4;
/// The fewest async read slots a store keeps
pub const LEAST_READ_SLOTS: usize = 8;
/// Read buffers a store with the full budget keeps
pub const LEAST_READ_BUFFERS: usize = 2;
/// The fewest write buffers a store keeps
pub const LEAST_WRITE_BUFFERS: usize = 2;

/// How many of each pooled part a store keeps
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Counts {
    pub scans: usize,
    pub read_slots: usize,
    pub read_buffers: usize,
    pub write_buffers: usize,
}

/// What a store is made of, which fixes the parts a budget can't shrink
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StoreShape {
    pub budget: ByteCount,
    pub roots: usize,
    /// The segments every volume's unmarked writes may take at once, added up
    pub open_segments: usize,
    /// The lifetime groups every volume may hold open at once, added up
    pub lifetime_groups: usize,
    pub volumes: usize,
    pub has_lz4: bool,
    pub widest_keys: u64,
    pub threads: ThreadCounts,
    pub has_ring: bool,
    pub disk_entry_bytes: ByteCount,
    pub floor_entry_bytes: ByteCount,
    pub workers: Option<usize>,
    /// The heap the declared indexes hold: complete tables with their block lines, and sorted indexes. The budget pays for them
    pub declared_bytes: ByteCount,
    /// A reader that writes nothing: no write buffers, no encoders, no jobs past its read threads
    pub is_read_only: bool,
    /// The bytes each slot's record windows hold: the largest window any volume takes
    pub window_bytes: usize,
    /// The bytes the volumes' seals plan under MOST_SEGMENT_BLOCKS' list, where a set segment size bounds the blocks they write
    pub seal_entries_saved: u64,
}

/// What a budget buys
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EngineSizing {
    pub segment_bytes: ByteCount,
    pub segment_rows: RowCount,
    pub shard: ShardSizing,
    pub spot_share: ByteCount,
    pub cache_share: ByteCount,
    pub record_share: ByteCount,
    pub value_share: ByteCount,
    pub summary_share: ByteCount,
    pub table_share: ByteCount,
    pub lz4_stored_limit: ByteCount,
    pub workers: usize,
    pub walk_slots: usize,
    pub least_budget: ByteCount,
    pub room: ByteCount,
    pub entry_line: ByteCount,
    pub counts: Counts,
    pub window_bytes: usize,
    pub is_read_only: bool,
}

impl StoreShape {
    /// Count the maintenance workers the store runs, which its volumes share
    pub fn workers(&self) -> usize {
        if self.is_read_only {
            return 0;
        }
        self.workers.or_else(|| self.threads.set_workers()).unwrap_or_else(default_workers)
    }

    /// Count the walk slots each volume has, and the rewrite seats beside its planner's lane
    pub fn walk_slots(&self) -> usize {
        self.workers().min(MOST_VOLUME_SLOTS)
    }

    /// Build the shape with the workers settled: the count a test set, or the most of one an online CPU the budget holds
    pub fn with_workers_settled(self) -> Self {
        if self.workers.is_some() || self.threads.set_workers().is_some() {
            return self;
        }
        let with_workers = |workers: usize| Self { workers: Some(workers), ..self };
        let fits = |workers: &usize| with_workers(*workers).least_budget() <= self.budget;
        with_workers((0..=default_workers()).rev().find(fits).unwrap_or(0))
    }

    /// Count the scans, read slots, read buffers and write buffers this budget runs: all of them at FULL_COUNTS_BUDGET and past
    /// it, a share in step with the budget below it, and never fewer read buffers than the workers read through
    pub fn counts(&self) -> Counts {
        let counted = |most: usize, least: usize| {
            let share = (most as u128 * u128::from(self.budget.0) / u128::from(FULL_COUNTS_BUDGET.0)) as usize;
            share.clamp(least, most)
        };
        let read_buffers = counted(READ_BUFFERS, LEAST_READ_BUFFERS).max(READERS_PER_WORKER * self.workers());
        Counts {
            scans: counted(MOST_SCANS, LEAST_SCANS),
            // the slots carry async reads to the read threads, and a store with none holds no slot
            read_slots: if self.threads.read_threads == 0 { 0 } else { counted(READ_SLOTS, LEAST_READ_SLOTS) },
            read_buffers,
            write_buffers: if self.is_read_only { 0 } else { counted(WRITE_BUFFERS, LEAST_WRITE_BUFFERS) },
        }
    }

    /// Count the slots every volume's open segments take
    pub fn open_slots(&self) -> usize {
        (self.open_segments * SLOTS_PER_OPEN + self.lifetime_groups).min(MAX_OPEN_SLOTS)
    }

    /// Add up what a store holds whatever its budget: buffers, pools, stacks and the spans a summary must keep
    pub fn fixed_bytes(&self) -> ByteCount {
        let volumes = self.volumes as u64;
        let counts = self.counts();
        // a reader takes in open rows through no queue and writes no group
        let queues = if self.is_read_only { 0 } else { (self.open_slots() as u64 * 2 + counts.write_buffers as u64) * GROUP_BYTES.0 };
        let gathers = counts.write_buffers as u64 * GATHER_BYTES as u64;
        // a reader fills no record window
        let windows = if self.is_read_only { 0 } else { self.open_slots() as u64 * 2 * self.window_bytes as u64 };
        let ring = if self.has_ring { (RING_SLOTS * RING_SLOT_BYTES) as u64 + RING_READS as u64 * RING_READ_BYTES } else { 0 };
        let reads = counts.read_buffers as u64 * READ_BUFFER_BYTES + ring;
        // each planner and each worker holds one footer reader of the job pool at a time, so none waits on a caller's
        let workers = self.workers() as u64;
        let job_reads = if self.is_read_only { 0 } else { (volumes + workers) * JOB_READER_BYTES };
        let scans = counts.scans as u64 * planned_scan_bytes(self.widest_keys, self.open_slots());
        let read_slots = counts.read_slots as u64 * (SLOT_KEY_BYTES + SLOT_KEYS * size_of::<usize>()) as u64;
        let large = if self.has_lz4 && !self.is_read_only { LZ4_STORED_LIMIT.0 } else { 0 };
        let encoders = counts.write_buffers as u64 * ENCODER_BYTES + large;
        let spans = 4 * MOST_DAMAGED_SPANS as u64 * (SPAN_KEY_BYTES as u64 * 2);
        let seats = volumes * self.walk_slots() as u64;
        let stacks = self.job_count() * JOB_STACK_PLAN_BYTES;
        // the pool holds both keys of every cover, and a seal job has room to write them all to a footer
        let cover_keys = 2 * MOST_RANGE_COVERS as u64 * self.widest_keys;
        let cover_fixed = (COVER_SLOT_BYTES + COVER_RECORD_BYTES) as u64 * MOST_RANGE_COVERS as u64;
        let covers = 2 * cover_keys + volumes * cover_fixed;
        let jobs = if self.is_read_only { 0 } else { SEAL_JOB_BYTES + WALK_JOB_BYTES + COMPACT_JOB_BYTES };
        let seal_saved = if self.is_read_only { 0 } else { self.seal_entries_saved };
        let worker_jobs = seats * LANE_JOB_BYTES;
        let declared = self.declared_bytes.0;
        ByteCount(queues + gathers + windows + reads + job_reads + scans + read_slots + encoders + covers + volumes * (jobs + spans) + worker_jobs + workers * WALK_JOB_BYTES + stacks + declared - seal_saved)
    }

    /// Count the jobs a store runs: the shared ones, its read threads, a seal job a volume and the workers
    pub fn job_count(&self) -> u64 {
        self.foreground_jobs() + self.workers() as u64
    }

    /// Count the jobs that serve calls: the shared ones, the read threads and a seal job a volume
    pub fn foreground_jobs(&self) -> u64 {
        if self.is_read_only {
            return self.threads.read_threads as u64;
        }
        SHARED_JOBS + self.threads.read_threads as u64 + self.volumes as u64
    }

    /// Read the fixed room the plan keeps for heap no part names, in place of a share of the budget: the rounding, and room a
    /// foreground job for its allocator's state
    pub fn room(&self) -> ByteCount {
        ByteCount(ROUNDING_ROOM + self.foreground_jobs() * JOB_ROOM_BYTES)
    }

    /// Read the smallest budget this store can run in, with the least entries its disks' tables hold
    pub fn least_budget(&self) -> ByteCount {
        ByteCount(self.fixed_bytes().0 + self.room().0 + self.floor_entry_bytes.0 + LEAST_SHARED_BYTES.0)
    }

    /// Read the entries line: what the disks ask for, up to the floor and a quarter of what the budget has past its least
    pub fn entry_line(&self) -> ByteCount {
        let cap = self.budget.0.saturating_sub(self.least_budget().0) / ENTRY_LINE_DIVISOR;
        ByteCount(self.disk_entry_bytes.0.min(self.floor_entry_bytes.0 + cap))
    }
}

impl EngineSizing {
    /// Share out what a budget leaves after the fixed parts
    pub fn for_store(shape: &StoreShape) -> Self {
        let open_slots = shape.open_slots();
        let budget = shape.budget;
        // the room and the entries come out before the shares
        let shared = budget.0.saturating_sub(shape.fixed_bytes().0 + shape.room().0 + shape.entry_line().0);
        let (spot_share, cache_share) = (share_of(shared, SPOT_SIXTEENTHS), share_of(shared, BLOCK_CACHE_SIXTEENTHS));
        let held = if budget >= FULL_BUDGET { HELD_VALUE_SIXTEENTHS } else { 0 };
        let index_share = share_of(shared, WRITE_INDEX_SIXTEENTHS - held);
        let rows_total = index_share / PLANNED_ROW_BYTES;
        Self {
            segment_bytes: segment_bytes_for(budget),
            segment_rows: RowCount(rows_total * SEAL_ROWS_EIGHTHS / 8),
            shard: shard_for_rows(rows_total, open_slots),
            spot_share: ByteCount(spot_share),
            cache_share: ByteCount(cache_share),
            record_share: ByteCount(share_of(shared, RECORD_CACHE_SIXTEENTHS)),
            value_share: ByteCount(share_of(shared, held)),
            summary_share: ByteCount(share_of(shared, SUMMARY_SIXTEENTHS)),
            table_share: ByteCount(share_of(shared, TABLE_SIXTEENTHS)),
            lz4_stored_limit: LZ4_STORED_LIMIT,
            workers: shape.workers(),
            walk_slots: shape.walk_slots(),
            least_budget: shape.least_budget(),
            room: shape.room(),
            entry_line: shape.entry_line(),
            counts: shape.counts(),
            is_read_only: shape.is_read_only,
            window_bytes: shape.window_bytes,
        }
    }

    /// Size the write index for the rows a segment of this mean record holds, with the seal's margin and twice that again for
    /// writers in flight, and give what it no longer needs to the record cache. A store of small rows, or one with nothing on
    /// disk, keeps the index as planned
    pub fn for_mean_record(self, mean_record: Option<u64>, volumes: u64) -> Self {
        let Some(mean_record) = mean_record.filter(|bytes| *bytes > 0) else {
            return self;
        };
        let planned_rows = self.segment_rows.0 * 8 / SEAL_ROWS_EIGHTHS;
        let segment_rows = self.segment_bytes.0.div_ceil(mean_record);
        // small rows that come after this open still seal at a fair size, since the index can't grow while open
        let wanted_rows = (segment_rows * 8 / SEAL_ROWS_EIGHTHS * INDEX_ROWS_MARGIN * volumes.max(1)).max(planned_rows / LEAST_INDEX_DIVISOR);
        if wanted_rows >= planned_rows {
            return self;
        }
        // the record cache takes what the index really frees, so the budget holds the same
        let shard = shard_for_rows(wanted_rows, self.shard.open_slots);
        let freed = (self.shard.heap_bytes().0.saturating_sub(shard.heap_bytes().0)) * SHARD_COUNT as u64;
        Self {
            segment_rows: RowCount(wanted_rows * SEAL_ROWS_EIGHTHS / 8),
            shard,
            record_share: ByteCount(self.record_share.0 + freed),
            ..self
        }
    }

    /// Size the write index for the rows the open segments of a set segment size can hold, a segment's bytes over its row
    /// floor with the seal's margin, and give the rest to the record cache. A cap under the rows only seals a segment sooner
    pub fn for_set_segments(self, segment_rows: Option<u64>, volumes: u64) -> Self {
        let Some(rows) = segment_rows else {
            return self;
        };
        let planned_rows = self.segment_rows.0 * 8 / SEAL_ROWS_EIGHTHS;
        let (wanted_rows, segment_rows) = capped_rows(rows, volumes);
        if wanted_rows >= planned_rows {
            return self;
        }
        let shard = shard_for_rows(wanted_rows, self.shard.open_slots);
        let freed = (self.shard.heap_bytes().0.saturating_sub(shard.heap_bytes().0)) * SHARD_COUNT as u64;
        Self {
            segment_rows: RowCount(segment_rows),
            shard,
            record_share: ByteCount(self.record_share.0 + freed),
            ..self
        }
    }

    /// Size a reader's write index for twice the open rows its listing found, and give the rest to the record cache. Past what
    /// the writer's share holds the index stays at that share, and rows past it refuse the load as they always have
    pub fn for_open_rows(self, rows: RowCount) -> Self {
        let planned_rows = self.segment_rows.0 * 8 / SEAL_ROWS_EIGHTHS;
        let wanted_rows = rows.0.saturating_mul(OPEN_ROWS_GROWTH);
        if wanted_rows >= planned_rows {
            return self;
        }
        let shard = shard_for_rows(wanted_rows, self.shard.open_slots);
        let freed = (self.shard.heap_bytes().0.saturating_sub(shard.heap_bytes().0)) * SHARD_COUNT as u64;
        Self {
            shard,
            record_share: ByteCount(self.record_share.0 + freed),
            ..self
        }
    }
}

/// Size one shard's pools for the rows the whole index holds
pub fn shard_for_rows(rows_total: u64, open_slots: usize) -> ShardSizing {
    let rows_per_shard = (rows_total / SHARD_COUNT as u64).max(64);
    let least_pages = (open_slots + 1) as u64;
    let page_rows = (PAGE_BYTES / 64) as u64;
    // the rows' pages, and a quarter more that kept values may take, so they never take a page a row needs
    let row_pages = (rows_per_shard / page_rows + 1) * 2;
    let pages = (row_pages + row_pages / 4).max(least_pages);
    let nodes = (rows_per_shard / (NODE_FANOUT as u64 / 2) + 1) * 2 + 64;
    ShardSizing {
        pages: PageCount(pages as u32),
        nodes: NodeCount(nodes as u32),
        table_slots: (rows_per_shard as usize * 2).next_power_of_two(),
        open_slots,
        buckets: (rows_per_shard as usize * OPEN_BUCKETS_PER_ROW).next_power_of_two(),
    }
}

/// Buckets of a shard's open counts for each row it holds, so most buckets near a scan are empty
pub const OPEN_BUCKETS_PER_ROW: usize = 4;

/// Read one share of what the fixed parts, the room and the entries leave, scaled so the shares together take the whole
pub const fn share_of(shared: u64, sixteenths: u64) -> u64 {
    shared / SHARED_SIXTEENTHS * sixteenths
}

/// Read the entries a volume's table plans for: the segments its disks hold at the segment size times the margin, in whole
/// shards, or nothing for a volume with a disk of no known size
pub fn disk_entries(disk: Option<ByteCount>, segment_bytes: ByteCount) -> Option<usize> {
    let segments = disk?.0.div_ceil(segment_bytes.0.max(1)) * ENTRY_MARGIN;
    Some((segments as usize).div_ceil(TABLE_SHARDS).max(LEAST_SHARD_ENTRIES) * TABLE_SHARDS)
}

/// Read the segment size a volume's entries plan at: the byte target, or a full segment's rows at the volume's row floor when
/// those come to less
pub fn assumed_segment_bytes(segment_bytes: ByteCount, segment_rows: RowCount, row_floor: u64) -> ByteCount {
    ByteCount(segment_bytes.0.min(segment_rows.0.saturating_mul(row_floor)).max(1))
}

/// Read the bytes an entry plans for on a volume with some columns
pub fn entry_cost(columns: usize) -> u64 {
    PLANNED_SEGMENT_BYTES + columns.max(1) as u64 * PLANNED_COLUMN_BYTES
}

/// Read the entries every table holds, whatever its disk
pub const fn floor_entries() -> usize {
    LEAST_SHARD_ENTRIES * TABLE_SHARDS
}

/// Scale a volume's entries past the floor by the room past the floor over what the disks ask past it, in whole shards
pub fn capped_entries(entries: usize, room: u64, asked: u64) -> usize {
    let past = entries.saturating_sub(floor_entries()) as u128;
    let scaled = (past * u128::from(room) / u128::from(asked.max(1))) as usize;
    (floor_entries() + scaled) / TABLE_SHARDS * TABLE_SHARDS
}

/// Read the disk bytes a volume's entries cover: the segments they plan for at the segment size, up to the disk's size
pub fn entry_cover(entries: usize, segment_bytes: ByteCount, disk: ByteCount) -> ByteCount {
    ByteCount((entries as u64 / ENTRY_MARGIN).saturating_mul(segment_bytes.0).min(disk.0))
}

/// Work out how many rows a segment can take and still fit the row blocks a complete slot can address, for a column's widest
/// key. A block closes with room left for its next row, and rows can reach their worst size
pub fn complete_row_limit(widest_key: usize) -> RowCount {
    let lines = MOST_SEGMENT_BLOCKS.0 as u64 * SEAL_LINE_EIGHTHS / 8;
    RowCount(lines * complete_block_rows(widest_key) as u64)
}

/// Work out the fewest rows a complete block closes with, for a column's widest key
pub fn complete_block_rows(widest_key: usize) -> usize {
    let room = COMPLETE_BLOCK_BYTES - BLOCK_TRAILER_BYTES - RESTART_ROOM - (ROW_ROOM_NUMBERS + widest_key);
    room / (WORST_ROW_NUMBERS + widest_key)
}

/// Work out how many rows a volume seals at. A seal frees its rows only as it ends, so with the index holding `T` rows, a seal
/// running at `S` rows a second and ingest at `I`, room lasts through the seal when it starts at `T * S / (S + I)` or below. No
/// measurement yet leaves the row limit
pub fn seal_point(limit: RowCount, seal_rate: u64, ingest_rate: u64) -> RowCount {
    if seal_rate == 0 || ingest_rate == 0 {
        return limit;
    }
    let index_rows = u128::from(limit.0) * 8 / u128::from(SEAL_ROWS_EIGHTHS);
    let point = index_rows * u128::from(seal_rate) / (u128::from(seal_rate) + u128::from(ingest_rate));
    let margined = (point * u128::from(SEAL_POINT_EIGHTHS) / 8) as u64;
    let least = (index_rows / u128::from(LEAST_SEAL_DIVISOR)) as u64;
    // a seal that ends inside the same time needs no room held for it
    let least_room = u128::from(seal_rate) / LEAST_ROOM_DIVISOR;
    let room_line = if least_room < u128::from(limit.0) { (index_rows - least_room) as u64 } else { index_rows as u64 };
    RowCount(margined.min(room_line).max(least).min(limit.0))
}

/// Work out a rate in rows a second, or nothing when no time has passed
pub fn rows_per_second(rows: RowCount, elapsed: u64) -> u64 {
    if elapsed == 0 {
        return 0;
    }
    (u128::from(rows.0) * u128::from(SECOND) / u128::from(elapsed)) as u64
}

/// Read the bytes a budget sets a segment at: the target once the budget reaches the default, and half the budget below it
pub fn segment_bytes_for(budget: ByteCount) -> ByteCount {
    let bytes = if budget >= FULL_BUDGET { SEGMENT_TARGET } else { ByteCount(budget.0 / 2) };
    ByteCount(bytes.0.min(MOST_SEGMENT_BYTES.0 / 2))
}

/// Rows the dead bits and the arena are sized for: the rows on disk at open with room to grow, and never fewer than the floor
pub fn target_rows(rows_on_disk: u64) -> u64 {
    rows_on_disk.saturating_mul(ARENA_GROWTH).max(floor_rows())
}

/// Bytes of dead bits for the rows on disk at open: a bit a target row, and a quarter more for each block's words rounding up
pub fn dead_bits_bytes(rows_on_disk: u64) -> u64 {
    target_rows(rows_on_disk) / 8 * 5 / 4
}

#[cfg(feature = "test-hooks")]
thread_local! {
    /// A floor a test sets for the stores it opens on its own thread, so a small one fills past it
    pub static TEST_FLOOR_ROWS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// Read the floor: a test's when it set one, else FLOOR_ROWS
#[cfg(feature = "test-hooks")]
pub fn floor_rows() -> u64 {
    match TEST_FLOOR_ROWS.with(std::cell::Cell::get) {
        0 => FLOOR_ROWS,
        rows => rows,
    }
}

/// Read the floor
#[cfg(not(feature = "test-hooks"))]
pub const fn floor_rows() -> u64 {
    FLOOR_ROWS
}

/// How the filter cache's room splits at open: what it keeps, and what the row and record caches take from it
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IdleSplit {
    pub filter_cache: u64,
    pub to_rows: u64,
    pub to_records: u64,
}

/// Split the filter cache's room. It holds only filters the arena has no room for, so its room past one stripe sits idle
/// while the arena holds every filter, and none is left over once it doesn't. The row cache takes the idle room in whole
/// stripes, up to the row blocks on disk with the arena's margin past its own share, and the record cache takes the rest
pub fn split_idle_room(cache_room: u64, stripe: u64, base_rows: u64, row_block_bytes: u64) -> IdleSplit {
    let idle = cache_room.saturating_sub(stripe);
    let wanted = row_block_bytes.saturating_mul(ARENA_GROWTH);
    let to_rows = idle.min(wanted.saturating_sub(base_rows)) / stripe * stripe;
    IdleSplit {
        filter_cache: cache_room - idle,
        to_rows,
        to_records: idle - to_rows,
    }
}

/// What one sorted index asks of the caches: its bytes, or its whole part of the cap for an empty index with no least rows
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IndexAsk {
    Bytes(ByteCount),
    Part,
}

/// Work out a sorted index's ask: the keys it held at open times the growth with the headroom past them, and at least its least
/// rows
pub fn index_ask(width: usize, keys: u64, least_rows: Option<RowCount>, headroom: ByteCount) -> IndexAsk {
    if keys == 0 && least_rows.is_none() {
        return IndexAsk::Part;
    }
    let rows = keys.saturating_mul(INDEX_GROWTH).max(least_rows.map_or(0, |least| least.0));
    IndexAsk::Bytes(ByteCount(planned_index_bytes(width, RowCount(rows)).0 + headroom.0))
}

/// Read the sorted indexes' cap: the spot, block and record caches past the floor they keep
pub const fn index_cap(caches: u64) -> u64 {
    caches - caches / 4 * CACHE_FLOOR_QUARTERS
}

/// Share the cap among the asks: each its ask while they fit, and each the same fraction of its ask past it. An empty index's
/// part is the cap over the indexes
pub fn share_indexes(asks: &[IndexAsk], cap: u64) -> Vec<u64> {
    let part = cap / asks.len().max(1) as u64;
    let wanted: Vec<u64> = asks
        .iter()
        .map(|ask| match ask {
            IndexAsk::Bytes(bytes) => bytes.0,
            IndexAsk::Part => part,
        })
        .collect();
    let total: u64 = wanted.iter().sum();
    if total <= cap {
        return wanted;
    }
    wanted.iter().map(|bytes| (u128::from(*bytes) * u128::from(cap) / u128::from(total)) as u64).collect()
}

#[cfg(feature = "test-hooks")]
thread_local! {
    /// A sim on its own thread sets this to give each sorted index exactly the rows it declared, so a small one fills and marks
    pub static EXACT_INDEX_ROWS: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Read whether this thread's stores give each sorted index exactly the rows it declared
#[cfg(feature = "test-hooks")]
pub fn is_index_exact() -> bool {
    EXACT_INDEX_ROWS.with(std::cell::Cell::get)
}

/// Read whether this thread's stores give each sorted index exactly the rows it declared, which no node build does
#[cfg(not(feature = "test-hooks"))]
pub const fn is_index_exact() -> bool {
    false
}

#[cfg(feature = "test-hooks")]
thread_local! {
    /// A headroom a sim sets for the stores it opens on its own thread, so a small index's pool follows its keys
    pub static TEST_INDEX_HEADROOM: std::cell::Cell<Option<u64>> = const { std::cell::Cell::new(None) };
}

/// Work out each sorted index's headroom: a quarter of the cap shared evenly among the indexes, and no more than INDEX_HEADROOM
/// each. A sim's own headroom takes its place when it set one
#[cfg(feature = "test-hooks")]
pub fn index_headroom(cap: u64, indexes: usize) -> ByteCount {
    TEST_INDEX_HEADROOM.with(std::cell::Cell::get).map_or_else(|| store_headroom(cap, indexes), ByteCount)
}

/// Work out each sorted index's headroom: a quarter of the cap shared evenly among the indexes, and no more than INDEX_HEADROOM
/// each
#[cfg(not(feature = "test-hooks"))]
pub const fn index_headroom(cap: u64, indexes: usize) -> ByteCount {
    store_headroom(cap, indexes)
}

/// Share a quarter of the cap evenly among the indexes, up to INDEX_HEADROOM each
pub const fn store_headroom(cap: u64, indexes: usize) -> ByteCount {
    let each = cap / 4 * HEADROOM_QUARTERS / if indexes == 0 { 1 } else { indexes as u64 };
    ByteCount(if each < INDEX_HEADROOM.0 { each } else { INDEX_HEADROOM.0 })
}

/// Work out the row blocks a seal can write for a segment of this many rows: a block holds at least fewest_rows_for_key of the
/// widest rows the volume's keys allow, a column change can close one early, and two more for the margin a merge keeps. Never more than
/// MOST_SEGMENT_BLOCKS
pub fn blocks_for_segment_rows(rows: u64, widest_key: usize) -> BlockCount {
    let blocks = rows.div_ceil(fewest_rows_for_key(widest_key)) + MAX_COLUMNS as u64 + 2;
    BlockCount(blocks.min(u64::from(MOST_SEGMENT_BLOCKS.0)) as u32)
}

/// Work out the most rows a segment can take and still seal into this many row blocks, the other side of blocks_for_segment_rows
pub fn rows_within_blocks(blocks: BlockCount, widest_key: usize) -> u64 {
    u64::from(blocks.0).saturating_sub(MAX_COLUMNS as u64 + 2) * fewest_rows_for_key(widest_key)
}

/// Work out the row blocks a volume's seal plans for. A store whose every volume sets its segment size caps the rows a segment
/// takes (for_set_segments), so the seal's list follows that cap, the same rows from the same function. A volume
/// with a complete column keeps MOST_SEGMENT_BLOCKS, since its seals feed the table
pub fn seal_blocks(config: &StoreConfig, volume: usize) -> BlockCount {
    let has_complete = config.columns.iter().any(|column| column.volume.0 as usize == volume && column.complete_rows().is_some());
    let capped = set_segment_rows(config).map(|rows| capped_rows(rows, config.volumes.len() as u64).1);
    match capped {
        Some(rows) if !has_complete => blocks_for_segment_rows(rows, config.widest_key(volume)),
        _ => MOST_SEGMENT_BLOCKS,
    }
}

/// Read the rows a segment of the largest set size can hold at the format's row floor, when every volume sets its size
pub fn set_segment_rows(config: &StoreConfig) -> Option<u64> {
    let sizes = config.volumes.iter().map(|volume| volume.segment_bytes.map(|bytes| bytes.0)).collect::<Option<Vec<u64>>>()?;
    sizes.into_iter().max().map(|bytes| bytes / FORMAT_ROW_FLOOR)
}

/// Work out the write index rows and the segment rows a set size caps a store at, from a segment's rows at the floor and the
/// volume count: the index holds three segments' worth with the seal's margin for every volume, and a segment seals at its share
pub fn capped_rows(segment_rows: u64, volumes: u64) -> (u64, u64) {
    let wanted_rows = (segment_rows * 8 / SEAL_ROWS_EIGHTHS * INDEX_ROWS_MARGIN * volumes.max(1)).max(1);
    (wanted_rows, wanted_rows * SEAL_ROWS_EIGHTHS / 8)
}

/// Check a volume's seal can write every block its segments can need: its segment row limit within what its list holds
pub fn check_seal_blocks(segment_rows: RowCount, blocks: BlockCount, widest_key: usize) -> std::io::Result<()> {
    if segment_rows.0 > rows_within_blocks(blocks, widest_key) {
        return Err(std::io::Error::other(format!(
            "a segment can take {} rows and its seal's list of {} blocks holds {}",
            segment_rows.0,
            blocks.0,
            rows_within_blocks(blocks, widest_key)
        )));
    }
    Ok(())
}

/// Count the fewest rows a row block holds when no key is wider than this: the block's room over the widest row it can take,
/// less one for the row that closes it
pub fn fewest_rows_for_key(widest_key: usize) -> u64 {
    let widest_row = ROW_ROOM_NUMBERS + widest_key + size_of::<u16>();
    (ROW_BLOCK_BYTES.saturating_sub(BLOCK_TRAILER_BYTES) / widest_row).saturating_sub(1).max(1) as u64
}
