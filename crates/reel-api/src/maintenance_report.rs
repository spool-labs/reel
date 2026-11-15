//! Each maintenance job's state and the store's reclaim counts

use crate::column_id::ColumnId;
use crate::units::{ByteCount, Nanoseconds, RowCount, SegmentCount, VolumeId};

/// Where the bytes reel writes come from
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WriteSource {
    /// Records written as they come in
    Records,
    /// Rows files
    Rows,
    /// A seal's row blocks, index and tail
    SealFooters,
    /// Records a merge carried into its outputs
    MergeRecords,
    /// A merge output's row blocks, index and tail
    MergeFooters,
    /// Bytes a merge wrote and then gave up
    GivenUp,
    /// Summary saves
    Summaries,
}

/// Every write source, in the order a report lists them
pub const WRITE_SOURCES: [WriteSource; 7] = [
    WriteSource::Records,
    WriteSource::Rows,
    WriteSource::SealFooters,
    WriteSource::MergeRecords,
    WriteSource::MergeFooters,
    WriteSource::GivenUp,
    WriteSource::Summaries,
];

impl WriteSource {
    /// The source's place in a report
    pub const fn index(self) -> usize {
        self as usize
    }
}

/// What a writer paused for
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PauseCause {
    /// Its volume's ration of the write index
    Ration,
    /// Room in the write index
    Index,
    /// A seal of a segment it could claim in
    SealDue,
    /// Another writer's room step on its segment
    RoomStep,
    /// Room in the write order's ring
    Ring,
    /// A segment another writer is making
    Creating,
    /// A segment the volume had no room to make
    NoSegment,
    /// A sync another writer is running
    Sync,
    /// Its slot's writing turn
    WritingTurn,
    /// The record window's turn
    RecordTurn,
    /// The other claims on a batch's slot
    OtherClaims,
    /// The next segment reservation
    Reservation,
}

/// Every pause cause, in the order a report lists them
pub const PAUSE_CAUSES: [PauseCause; 12] = [
    PauseCause::Ration,
    PauseCause::Index,
    PauseCause::SealDue,
    PauseCause::RoomStep,
    PauseCause::Ring,
    PauseCause::Creating,
    PauseCause::NoSegment,
    PauseCause::Sync,
    PauseCause::WritingTurn,
    PauseCause::RecordTurn,
    PauseCause::OtherClaims,
    PauseCause::Reservation,
];

impl PauseCause {
    /// The cause's place in a report
    pub const fn index(self) -> usize {
        self as usize
    }
}

/// Every kind of job, in the order a report lists them
pub const JOB_KINDS: [JobKind; 6] = [
    JobKind::RowWriteOut,
    JobKind::Seal,
    JobKind::DeadByteWalk,
    JobKind::RangeDeleteApply,
    JobKind::Compaction,
    JobKind::SpotCacheUpkeep,
];

/// A kind of maintenance job
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum JobKind {
    /// Row groups reaching the kernel
    RowWriteOut,
    /// Open segments becoming sealed ones
    Seal,
    /// Counting superseded versions after a seal
    DeadByteWalk,
    /// Unlinking segments a range delete covers
    RangeDeleteApply,
    /// Merging and rewriting sealed segments
    Compaction,
    /// Keeping the spot cache in its budget
    SpotCacheUpkeep,
}

impl JobKind {
    /// Read this kind's place in `JOB_KINDS`
    pub const fn index(self) -> usize {
        self as usize
    }
}

/// Every kind of maintenance unit, in the order a report lists them
pub const UNIT_KINDS: [UnitKind; 19] = [
    UnitKind::WalkBlock,
    UnitKind::MergeRows,
    UnitKind::StripeCopy,
    UnitKind::BlockMove,
    UnitKind::Footer,
    UnitKind::Install,
    UnitKind::UnlinkCheck,
    UnitKind::HeldFrees,
    UnitKind::CoverApply,
    UnitKind::CoverSweep,
    UnitKind::FilterLoad,
    UnitKind::ParkedClear,
    UnitKind::Pick,
    UnitKind::SummaryCopy,
    UnitKind::SummarySave,
    UnitKind::Start,
    UnitKind::HeldDrain,
    UnitKind::SlotRefill,
    UnitKind::SlotSweep,
];

/// A kind of maintenance unit. A job runs as units, and each kind's work is counted in what its line says
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum UnitKind {
    /// One row block of the dead-byte walk, in rows
    WalkBlock,
    /// A step of a merge's or rewrite's row decisions, in rows
    MergeRows,
    /// A step of a record rewrite's stripe copy, in records
    StripeCopy,
    /// A step moving a rewrite's row blocks down to its records' end, in row blocks
    BlockMove,
    /// Writing a rewrite's footer, in row blocks
    Footer,
    /// A rewrite's install, in segments listed and retired
    Install,
    /// A step of the check that every row of a segment is dead before it unlinks, in rows
    UnlinkCheck,
    /// A step freeing a flagged segment's large dead records, in rows
    HeldFrees,
    /// A step of a segment's range deletes, in steps
    CoverApply,
    /// Dropping the range deletes nothing needs any more, in covers
    CoverSweep,
    /// A step loading filters into the arena, in filter blocks
    FilterLoad,
    /// Giving back a file no read holds any more, in files
    ParkedClear,
    /// A look for the next job, in looks
    Pick,
    /// Copying the summary out under its lock for a save, in bytes
    SummaryCopy,
    /// Saving the whole summary, the copy and the write, in bytes
    SummarySave,
    /// A rewrite mapping its region, its pages faulted in, and opening its output, on the thread that runs it, in bytes mapped
    Start,
    /// Landing one held block of a rewrite's inputs on its outputs before the install, in rows
    HeldDrain,
    /// Giving rows past a complete table's mark their slots once it has room again, in row blocks
    SlotRefill,
    /// Clearing a complete table's slots of gone segments, in buckets
    SlotSweep,
}

impl UnitKind {
    /// Read this kind's place in `UNIT_KINDS`
    pub const fn index(self) -> usize {
        self as usize
    }
}

/// Every kind of pick that reads a volume's summary under its lock, in the order a report lists them
pub const PICK_KINDS: [PickKind; 6] = [PickKind::Rewrite, PickKind::Overlap, PickKind::Walk, PickKind::Dead, PickKind::Filter, PickKind::Cover];

/// A kind of pick. Each holds the summary's lock, which a seal and an install also take
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum PickKind {
    /// A rewrite lane's pick of the segments a record rewrite wins most back from
    Rewrite,
    /// The planner's pick: an overlap merge first, then a record rewrite
    Overlap,
    /// The dead-byte walk's pick of its next segment
    Walk,
    /// The pick of a wholly dead segment to unlink
    Dead,
    /// The filter loader's pick of a segment the arena lacks
    Filter,
    /// A range delete's look for the next segment under it
    Cover,
}

impl PickKind {
    /// Read this kind's place in `PICK_KINDS`
    pub const fn index(self) -> usize {
        self as usize
    }
}

/// The largest unit of one kind since open, in work and in wall time, and the sums over every unit. The two largest can come
/// from different units
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct UnitReading {
    /// The most work one unit did
    pub work: u64,
    /// The longest one unit ran
    pub time: Nanoseconds,
    /// The work every unit did
    pub work_sum: u64,
    /// The time every unit ran
    pub time_sum: Nanoseconds,
    /// The most thread CPU one unit took, where a test build reads it, so a long unit shows whether it worked or waited
    pub cpu: Nanoseconds,
}

/// What a job is doing
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum JobState {
    /// Nothing to do
    Idle,
    /// Working
    Running,
    /// Moved nothing for an hour while work stands
    Stalled,
    /// Ended by close
    Stopped,
}

/// One job's state and totals
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct JobReport {
    pub kind: JobKind,
    pub state: JobState,
    pub bytes_moved: ByteCount,
    pub last_run: Option<Nanoseconds>,
}

/// One volume's readings a node roll watches
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct VolumeReading {
    pub volume: VolumeId,
    pub segment_count: SegmentCount,
    /// Rows in the write index not yet sealed, and the open segments that hold them: what a scan meets beside the sealed segments
    pub rows_in_index: RowCount,
    pub open_segments_with_rows: SegmentCount,
    /// Range deletes waiting in the pool, looks that left one there because a cue below it could read rows under it, and looks
    /// given up because a rewrite or an unlink landed while they ran
    pub pending_covers: u64,
    pub covers_held_by_cues: u64,
    pub settle_retries: u64,
    pub used_bytes: ByteCount,
    pub is_spinning: bool,
    /// The class came from the config, where a false here means it was detected
    pub is_device_set: bool,
    /// Rewrites each lane has finished: the planner's, then a worker's each
    pub rewrites_by_lane: [u64; MOST_REPORTED_LANES],
    /// Lifetime groups open now
    pub lifetime_groups: usize,
    /// Segments of marked records sealed since open
    pub marked_seals: u64,
    /// Record bytes those segments held at seal
    pub marked_seal_bytes: ByteCount,
    /// Marked batches that held a delete, which went to the unmarked segments with their marks dropped
    pub marks_dropped: u64,
    /// Marked segments rewritten at the early bar, while their highest mark stood
    pub early_bar_rewrites: u64,
    /// Marked segments rewritten at the plain bar, once the current mark had passed their highest
    pub plain_bar_rewrites: u64,
    pub longest_seal_steps: SealSteps,
    /// Rewrites that copied in key order, reading records at random
    pub key_order_copies: u64,
    /// Rewrites that copied each input's records in offset order
    pub offset_order_copies: u64,
    /// Reads the offset-order copy took a run of records in
    pub span_reads: u64,
    /// Records those reads took, so a run's mean length is these over the reads
    pub span_records: u64,
    /// Bytes those reads took between their records
    pub span_gap_bytes: ByteCount,
    /// Records the offset-order copy read alone, with no neighbour near enough
    pub lone_copies: u64,
    /// Records a merge read one at a time as their keys came, in key order or kept on the second look
    pub keyed_copies: u64,
    /// Row-block bytes merges read in their deciding, placing and copying passes
    pub footer_reads: [ByteCount; 3],
    /// The offset-order copy's reads by the records each took, bucket b holding reads of 2^b up to 2^(b+1) records
    pub span_runs: [u64; SPAN_RUN_BUCKETS],
    /// Whole summary saves since open
    pub summary_saves: u64,
    /// Segments the table holds now
    pub entries_used: SegmentCount,
    /// Segments the table has room for, which the disk sets when its size is known
    pub entry_room: SegmentCount,
    /// The segment size the entries' cover assumes: the byte target, or a full segment's rows at the row floor when that is less
    pub cover_segment_bytes: ByteCount,
    /// The disk bytes the table's room holds at the mean size of the segments it holds now, the real headroom beside the cover
    pub cover_at_mean: ByteCount,
    /// Bytes each entry plans for, by the volume's columns
    pub entry_bytes: ByteCount,
    /// Times a seal stopped on a full table or directory, once for each stop until a seal lands again
    pub seal_stops: u64,
    /// Looks for rewrite work, by what each came to, in the order of `PickOutcome`
    pub picks: [u64; PICK_OUTCOMES],
    /// What finished rewrites took off the disk and wrote back
    pub rewrite_bytes: RewriteBytes,
    /// What the pace earned, spent and refused
    pub pace: PaceReading,
    /// Walk blocks whose counts landed
    pub walk_blocks: u64,
    /// How long the writes took to fill the last sealed segment
    pub fill_interval: Nanoseconds,
    /// How long a merge took for each input, lately
    pub merge_interval: Nanoseconds,
    /// Time left until writes count as stopped and record rewrites may run
    pub ingest_left: Nanoseconds,
    /// Seals that ended a segment writes had gone quiet on
    pub idle_seals: u64,
    /// Overlap merges that started while another overlap merge ran, overlap merge inputs an earlier merge wrote, and every overlap
    /// merge input
    pub overlap_beside: u64,
    pub overlap_output_inputs: u64,
    pub overlap_inputs: u64,
    /// Bytes claims took and gave up without landing: a gap in the segment, or a record with no row
    pub unlanded_bytes: u64,
    /// Quiet open segments that gave their room ahead back
    pub rooms_given_back: u64,
    /// A test holds compaction off, which only a build with the test hooks can, so a bench run held by accident shows it
    pub is_compaction_held: bool,
    /// Where the volume's disk goes: live and dead record bytes in sealed segments, bytes written to open ones, and room open
    /// segments hold ahead of their writes
    pub live_bytes: ByteCount,
    pub dead_bytes: ByteCount,
    pub open_bytes: ByteCount,
    pub preallocated: ByteCount,
    /// Dead bytes the walk has counted, of which some sit on segments whose dead bits only ask and some under a live cue, and
    /// the record bytes of segments the walk hasn't reached, whose dead share nobody knows yet
    pub dead_counted: ByteCount,
    pub unwalked_bytes: ByteCount,
    pub inexact_dead: ByteCount,
    pub cue_held_dead: ByteCount,
    /// Rows of walked segments with dead bits, and rows of those born when the pool was full, each of which asks: past the
    /// floor a store that grew since open answers on the slow path, and this says how much of it does
    pub rows_with_bits: u64,
    pub rows_without_bits: u64,
    /// Merges that wrote past their promise and took more room, which a right estimate never lets happen
    pub promises_outgrown: u64,
}

/// What a rewrite lane's look for work came to. The overlap misses count beside the look's own outcome
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PickOutcome {
    /// The volume could start no rewrite: one runs on a spinning disk, outputs are stray, or the park is full
    Refused,
    /// A segment whose rows need a rebuild
    Rebuild,
    /// An overlap merge at a key past the bound
    Overlap,
    /// No key past the bound among the segments a merge can take
    OverlapMiss,
    /// A key past the bound with one segment there a merge can take
    OverlapLone,
    /// A walk target stands, so only an overlap merge could start
    WalkTarget,
    /// A segment a full cover pool forced
    Forced,
    /// A record rewrite of the segments that win back the most
    Record,
    /// Nothing was ready
    Nothing,
    /// An overlap merge or record rewrite waited for quiet, since the volume takes writes hot and has room to spare
    Deferred,
    /// A record rewrite waited behind an overlap merge that runs or can start
    Behind,
}

/// Outcomes a look for rewrite work can come to
pub const PICK_OUTCOMES: usize = 11;

impl PickOutcome {
    /// Read this outcome's place in a reading
    pub const fn index(self) -> usize {
        self as usize
    }
}

/// What finished rewrites took off the disk and wrote back
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RewriteBytes {
    /// File bytes of the inputs that left the disk
    pub inputs_gone: ByteCount,
    /// File bytes of the outputs
    pub written: ByteCount,
    /// Dead bytes the inputs that left held
    pub dead_gone: ByteCount,
    /// Dead bytes the outputs carry
    pub dead_kept: ByteCount,
    /// Rewrites that wrote at least the bytes their leaving inputs held
    pub unreclaimed: u64,
    /// Bytes the outputs took, by what the rewrite was: a record rewrite whose inputs won back under three quarters of their
    /// bytes, one that won back three quarters or more, an overlap merge, and a rebuild or forced merge
    pub written_by_share: [ByteCount; SHARE_KINDS],
    /// Bytes past the records the outputs of each kind took: footers, row blocks and summaries, which a merge that moved no
    /// records would still write
    pub footers_by_share: [ByteCount; SHARE_KINDS],
    /// Input bytes rewrites of each of those kinds took off the disk
    pub gone_by_share: [ByteCount; SHARE_KINDS],
    /// Rewrites of each of those kinds
    pub rewrites_by_share: [u64; SHARE_KINDS],
    /// File bytes whole unlinks took off the disk, which write nothing
    pub unlinked: ByteCount,
}

/// The pace that holds busy callers' record rewrites under half dead to the dead bytes the walk counts
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PaceReading {
    /// Copy bytes for each thousand dead bytes
    pub per_mille: u64,
    /// Copy bytes paced rewrites may still start, below zero while a rewrite wrote past its estimate
    pub credit: i64,
    /// Dead bytes the walk counted, which earn the credit
    pub dead_counted: ByteCount,
    /// Bytes paced rewrites wrote
    pub paced_written: ByteCount,
    /// Record picks the pace put off
    pub refusals: u64,
    /// Paced picks the space target put off, the volume under it
    pub gate_refusals: u64,
    /// The space target, dead bytes a rewrite must win back as thousandths of the live bytes
    pub target_per_mille: u64,
    /// Dead bytes only a rewrite can win back: walked, not freed in place, in segments not wholly dead
    pub gate_dead: ByteCount,
    /// Live record bytes, an unwalked segment's all counted live
    pub live: ByteCount,
    /// Dead bytes the record rewrites in flight will win back
    pub gate_in_flight: ByteCount,
    /// Credit the cap turned away
    pub spilled: ByteCount,
    /// Rows a pick held to the half bar, their counts not trusted while a segment below them was unwalked
    pub trust_fallbacks: u64,
    /// Dead bytes the walk in flight has charged, which no merge can drop until it ends: about one target's kills at most
    pub unbacked: ByteCount,
    /// Asks for a rewrite look the planner heard, so a burst of picks shows whether it came from asks or from its own looping
    pub asks_heard: u64,
}

/// The kinds a rewrite's written bytes are counted by: record rewrites under three quarters dead, at three quarters and up,
/// overlap merges, and rebuilds and forced merges
pub const SHARE_KINDS: usize = 4;
/// A record rewrite whose inputs won back under three quarters of their bytes
pub const RECORD_UNDER_EARLY_BAR: usize = 0;
/// A record rewrite whose inputs won back three quarters or more
pub const RECORD_PAST_EARLY_BAR: usize = 1;
/// An overlap merge
pub const OVERLAP_MERGE: usize = 2;
/// A rebuild or a forced merge
pub const OTHER_MERGE: usize = 3;

/// What the maintenance jobs' regions have held, and what admission found at its last start
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct MaintenanceMemory {
    /// Bytes the jobs' regions take now, past the fixed memory the memory report counts
    pub now: ByteCount,
    /// The most bytes the regions have taken at once
    pub peak: ByteCount,
    /// Bytes every region has mapped since open, whose pages fault in at their jobs' starts and at no step after
    pub mapped_total: ByteCount,
    /// The host's free bytes the last start read, no more than the control group's room
    pub free_at_start: ByteCount,
    /// The bytes the last start kept back for the page cache
    pub reserve_at_start: ByteCount,
    /// Starts admitted
    pub admits: u64,
    /// Starts that found too little room and waited
    pub waits: u64,
    /// Starts admitted past the room because no region was held, so reclaim never stops
    pub floor_admits: u64,
    /// Pushes past a list's capacity in this process, each a bound that came out short
    pub lists_full: u64,
    /// The largest need a rewrite was admitted with
    pub rewrite_need: ByteCount,
    /// The most bytes a rewrite's lists wrote
    pub rewrite_touched: ByteCount,
    /// The largest need a walk target was admitted with
    pub walk_need: ByteCount,
    /// The most bytes a walk target's counts wrote
    pub walk_touched: ByteCount,
    /// Bytes the walk holds from open: its block counters and the frees a cue holds back
    pub walk_fixed: ByteCount,
}

/// The scheduling class a maintenance worker's thread runs in, as the thread read it back
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum PriorityClass {
    /// The platform gave no reading, or the backend sets no class
    #[default]
    Unknown,
    /// The normal class, the one callers run in
    Normal,
    /// Linux's batch class, which a waking caller preempts
    Batch,
    /// macOS's utility QoS, below a caller's default class and above background
    Utility,
}

/// The class and nice value a maintenance worker runs at
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct WorkerPriority {
    /// The scheduling class
    pub class: PriorityClass,
    /// The nice value, 19 the lowest normal one
    pub nice: i32,
}

/// Buckets the offset-order copy's run lengths fall in, a power of two each, the last holding every longer run
pub const SPAN_RUN_BUCKETS: usize = 16;

/// Lanes a volume reading has room for. Workers past this show in the last one
pub const MOST_REPORTED_LANES: usize = 16;

/// How a complete column stands: the slots it uses and has, the rows that found no slot, and whether those put a mark on it.
/// A marked table sends reads past it to the footers until a refill clears the mark. A read-only view has no table, and reads
/// through the footers. Frees kept counts slots a walk left standing because another key of their tag might still need them: a
/// count that keeps growing holds slots the table can't get back until their segments go
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CompleteReading {
    pub column: ColumnId,
    pub has_table: bool,
    pub used: RowCount,
    pub capacity: RowCount,
    pub rows_past_capacity: RowCount,
    pub is_marked: bool,
    pub frees_kept: u64,
}

/// How a sorted column's index stands: its entries, graves among them, the rows it was told to hold, the rows past its room,
/// and whether those put a mark on it. A marked index sends every get and scan of its column to the segments until a refill
/// clears the mark, so entries near the told rows say the next writes go past it
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SortedReading {
    pub column: ColumnId,
    pub used: RowCount,
    pub capacity: RowCount,
    pub rows_past_capacity: RowCount,
    pub is_marked: bool,
}

/// How a column that declares a least value stands: the values written under it since open, which land as any other and size
/// nothing
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct LeastValueReading {
    pub column: ColumnId,
    pub least_value: ByteCount,
    pub values_under_least: u64,
}

/// Every job, the reclaim counts and the readings a node roll stops on, in buffers the caller reuses
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MaintenanceReport {
    pub jobs: [JobReport; JOB_KINDS.len()],
    /// Bytes written since open, by where they came from
    pub written: [ByteCount; WRITE_SOURCES.len()],
    /// Pauses writers took since open, by what they waited for
    pub pauses: [u64; PAUSE_CAUSES.len()],
    /// The longest a writer took to reserve room ahead of its claims
    pub longest_room_step: Nanoseconds,
    /// The most planner steps in a row that ended before the walk's turn while a walk was pending
    pub longest_walkless_steps: u64,
    /// The kind of unit behind that longest run, so a long one names its cause
    pub longest_walkless_kind: Option<UnitKind>,
    /// Reads the record cache answered
    pub record_cache_hits: u64,
    /// Reads it missed
    pub record_cache_misses: u64,
    /// Values it took in on a miss
    pub record_cache_admits: u64,
    /// Values it holds now
    pub record_cache_values: u64,
    /// Walk lookups through the cursor: segments it pinned, runs it copied again, rows it answered, rows it sent to the table
    pub cursor_lookups: [u64; 4],
    /// Merge lookups through the cursor, as above, then merge rows asked outside the merge and rows an unwalked merge settled alone
    pub merge_lookups: [u64; 6],
    /// Dead bits over every volume: pool bytes, bytes in use, segments with bits, then segments made inexact by an unknown place,
    /// a full charge list, damage, a short pool, counts that came with no bits and a missed charge, then merge rows a set bit
    /// dropped, a clear bit kept, asked as well, a set bit the ask found no newer row for, and a clear bit the ask found one for
    pub dead_bits: [u64; 14],
    /// Rows on disk in segments whose bits are exact, and all rows on disk
    pub bit_rows: [u64; 2],
    /// Takes from the record and footer reader pools that queued, and the wakes their lines sent
    pub reader_waits: u64,
    pub reader_wakes: u64,
    /// Heads of the reader pools' lines that woke to find their item taken ahead of them
    pub reader_passes: u64,
    /// Keys a get_many answered from records its batch read together
    pub batched_gets: u64,
    /// Chunks whose stored piece sums were damaged while the chunk's bytes were whole, which costs a window its fast way
    pub damaged_sums: u64,
    /// Scan sources seated, and those seated together in their cached blocks with their cache misses overlapped
    pub scan_seats: u64,
    pub seats_together: u64,
    /// Open trees a scan's heads descended together, their cache misses overlapped
    pub heads_together: u64,
    /// Values scans took from the record cache with no record read, while it could hold most of a volume
    pub scan_cache_hits: u64,
    /// Block cache inserts that found their shard held by a visit and kept nothing
    pub cache_inserts_skipped: u64,
    /// Gets a sorted index answered with no footer search, and gets on a sorted column that asked the footers: behind the
    /// newest sealed row, at an entry with no pinned segment, or past the index's mark
    pub sorted_answers: u64,
    pub sorted_footer_asks: u64,
    /// Scan keys whose sealed row came from a sorted index's batch
    pub sorted_scan_keys: u64,
    /// Runs of index entries a batch located in one loop
    pub sorted_runs: u64,
    /// Output row blocks a merge's install couldn't read to move sorted entries
    pub sorted_moves_missed: u64,
    /// Sorted images open took and refused
    pub sorted_images_taken: u64,
    pub sorted_images_refused: u64,
    /// Sorted image saves a close gave up on
    pub sorted_image_saves_failed: u64,
    /// Walk block lookups run together: blocks, candidates asked, candidates whose range held the whole block, per-key checks
    pub block_lookups: [u64; 4],
    pub volumes: Vec<VolumeReading>,
    pub complete_columns: Vec<CompleteReading>,
    pub sorted_columns: Vec<SortedReading>,
    pub least_value_columns: Vec<LeastValueReading>,
    pub graves_at_open: RowCount,
    pub deletes_since_open: RowCount,
    pub graves_dropped: RowCount,
    pub graves_carried: RowCount,
    pub segments_rewritten: SegmentCount,
    pub segments_unlinked: SegmentCount,
    pub rewrites_reclaiming_nothing: SegmentCount,
    pub corruption_errors: RowCount,
    pub damaged_segments: SegmentCount,
    pub summary_rows_dropped: RowCount,
    /// Summary rows the pick orders had no slot for, which no pick sees, and which stays none
    pub summary_rows_unordered: RowCount,
    /// The turns the walk stood aside while a rewrite ran
    pub walks_held: u64,
    /// Waits of a block whose charges land on an input a rewrite has claimed, none until the held bits come
    pub walk_claim_waits: u64,
    /// The time those waits held the walk, in all
    pub walk_claim_held: Nanoseconds,
    /// The longest one wait held the walk
    pub walk_claim_longest: Nanoseconds,
    /// Held blocks the drains read, each once
    pub held_drain_blocks: u64,
    /// The bytes those reads took
    pub held_drain_bytes: ByteCount,
    pub refused_reads: RowCount,
    pub reclaim_retries: RowCount,
    pub segments_parked: SegmentCount,
    pub empties_left: SegmentCount,
    pub stray_outputs: SegmentCount,
    /// How long the filter arena took to fill after open, which is the store's restart window
    pub arena_fill: Option<Nanoseconds>,
    /// Unlinks refused because a row the counts called dead stands live, which is always none
    pub unlinks_refused: SegmentCount,
    /// Async reads complete through the ring, and through the blocking pool when this is false
    pub has_ring: bool,
    pub longest_units: [UnitReading; UNIT_KINDS.len()],
    /// The longest one pick of each kind held a volume's summary since open
    pub longest_holds: [Nanoseconds; PICK_KINDS.len()],
    /// The maintenance jobs' regions and their admission
    pub memory: MaintenanceMemory,
    /// Maintenance workers the store runs, one an online CPU across its volumes
    pub workers: u64,
    /// Workers holding a walk block or a rewrite now
    pub active_workers: u64,
    /// The priority the workers run at
    pub worker_priority: WorkerPriority,
}

impl JobReport {
    /// Start an idle job with nothing moved
    pub fn new(kind: JobKind) -> Self {
        Self {
            kind,
            state: JobState::Idle,
            bytes_moved: ByteCount::default(),
            last_run: None,
        }
    }
}

impl MaintenanceReport {
    /// Forget the volume readings and keep the buffer
    pub fn clear(&mut self) {
        self.volumes.clear();
        self.complete_columns.clear();
        self.sorted_columns.clear();
        self.least_value_columns.clear();
    }

    /// Start a report with one idle entry per kind of job
    pub fn new() -> Self {
        Self {
            jobs: JOB_KINDS.map(JobReport::new),
            written: [ByteCount(0); WRITE_SOURCES.len()],
            pauses: [0; PAUSE_CAUSES.len()],
            longest_room_step: Nanoseconds(0),
            longest_walkless_steps: 0,
            longest_walkless_kind: None,
            record_cache_hits: 0,
            record_cache_misses: 0,
            record_cache_admits: 0,
            record_cache_values: 0,
            cursor_lookups: [0; 4],
            merge_lookups: [0; 6],
            dead_bits: [0; 14],
            bit_rows: [0; 2],
            reader_waits: 0,
            reader_wakes: 0,
            reader_passes: 0,
            batched_gets: 0,
            damaged_sums: 0,
            scan_seats: 0,
            seats_together: 0,
            heads_together: 0,
            scan_cache_hits: 0,
            cache_inserts_skipped: 0,
            sorted_answers: 0,
            sorted_footer_asks: 0,
            sorted_scan_keys: 0,
            sorted_runs: 0,
            sorted_moves_missed: 0,
            sorted_images_taken: 0,
            sorted_images_refused: 0,
            sorted_image_saves_failed: 0,
            block_lookups: [0; 4],
            volumes: Vec::new(),
            complete_columns: Vec::new(),
            sorted_columns: Vec::new(),
            least_value_columns: Vec::new(),
            graves_at_open: RowCount::default(),
            deletes_since_open: RowCount::default(),
            graves_dropped: RowCount::default(),
            graves_carried: RowCount::default(),
            segments_rewritten: SegmentCount::default(),
            segments_unlinked: SegmentCount::default(),
            rewrites_reclaiming_nothing: SegmentCount::default(),
            corruption_errors: RowCount::default(),
            damaged_segments: SegmentCount::default(),
            summary_rows_dropped: RowCount::default(),
            summary_rows_unordered: RowCount::default(),
            walks_held: 0,
            walk_claim_waits: 0,
            walk_claim_held: Nanoseconds(0),
            walk_claim_longest: Nanoseconds(0),
            held_drain_blocks: 0,
            held_drain_bytes: ByteCount(0),
            refused_reads: RowCount::default(),
            reclaim_retries: RowCount::default(),
            segments_parked: SegmentCount::default(),
            empties_left: SegmentCount::default(),
            stray_outputs: SegmentCount::default(),
            arena_fill: None,
            unlinks_refused: SegmentCount::default(),
            has_ring: false,
            longest_units: [UnitReading::default(); UNIT_KINDS.len()],
            longest_holds: [Nanoseconds(0); PICK_KINDS.len()],
            memory: MaintenanceMemory::default(),
            workers: 0,
            active_workers: 0,
            worker_priority: WorkerPriority::default(),
        }
    }
}

impl Default for MaintenanceReport {
    fn default() -> Self {
        Self::new()
    }
}

/// The longest each step of a seal has run since open, each a stretch with no room coming back on its own
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SealSteps {
    /// Waiting for the segment's writers to land
    pub drain: Nanoseconds,
    /// One step of the walk
    pub walk_step: Nanoseconds,
    /// Writing and syncing the footer
    pub footer: Nanoseconds,
    /// Listing the segment and saving its summary
    pub install: Nanoseconds,
    /// One step of cleaning the write index
    pub clean_step: Nanoseconds,
}
