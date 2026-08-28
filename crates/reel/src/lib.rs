//! A log-structured key-value store engine
//!
//! One append-only log of segment files over one or more volumes, holding every
//! column behind an index and serving them behind the store trait.

pub mod append;
pub mod compaction;
pub mod config;
pub mod engine;
pub mod error;
pub mod format;
pub mod hold;
pub mod index;
pub mod io;
pub mod reel;
pub mod report;
pub mod sync;
pub mod units;

pub use reel_core::{
    BatchOp, CfDiskUsage, Column, Direction, DiskVolume, Error as StoreError, KeyValue,
    Result as StoreResult, Store, StoreIter, StoreVolume, TypedStore, Value, WriteBatch,
};

pub use append::{Appender, BatchRecord, BatchWrite, Committed, DrainDepth, Durability, FlushTurn};
pub use compaction::compactor::{CompactionCounters, Compactor, EraseReport};
pub use compaction::merge::MergeReport;
pub use compaction::pressure::{GcPressure, GcTier, RateLimiter};
pub use config::{
    CompactRate, FenceResidency, HotIndex, IndexResidency, IoBackend, PointReads, Preallocate,
    RangedReads, ReelConfig, RepairPath, RingTuning, RingWait, ShardShapes, SyncPolicy,
    ThreadBudget, VolumeClass, VolumeSpec, DEFAULT_FD_CACHE, MAP_EVERYTHING,
};
pub use engine::index_checkpoint::IndexCheckpoint;
pub use engine::{CompactPass, RecordWrite, ReelStore, Totals};
pub use error::{ReelError, Result};
pub use format::column::{
    inline_bytes, Codec, ColumnId, ColumnSet, ColumnSpec, InlineBytes, KeyBytes, KeyRef, KeyWidth,
    MapShape, RecordKey, INLINE_KEY_LEN, INLINE_MAX, MARK_LEN, MAX_KEY_LEN, SHORT_KEY_LEN,
};
pub use index::counters::{ProbeCounts, ReadCounters, SegmentBytes, SegmentTable};
pub use index::entry::Entry;
pub use index::lockfile::OwnershipLock;
pub use index::map::ReelIndex;
pub use index::opentable::OpenTable;
pub use index::page::KeyPage;
pub use index::persisted::PersistedIndex;
pub use index::playback::{PlaybackCursor, Way};
pub use index::recovery::{rebuild_reel, RebuiltReel};
pub use io::ServingBackend;
pub use reel::checkpoint::Checkpoint;
pub use reel::cue::{CuePoint, CuePoints};
pub use reel::tail::Tail;
pub use reel::volumes::Volumes;
pub use reel::{segment_file_name, Reel, ReelShared, SEGMENT_SUFFIX};
pub use sync::tension::{Tension, Wait};
pub use units::ByteCount;
