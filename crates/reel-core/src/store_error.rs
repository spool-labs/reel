//! Every way a store call can fail

use std::io::Error as IoError;
use std::path::PathBuf;

use thiserror::Error;

use crate::column_id::ColumnId;
use crate::units::{ByteCount, ProcessId, RowCount, VolumeId};

/// A failed store call
#[derive(Debug, Error)]
pub enum StoreError {
    #[error("column {column:?} is not declared")]
    UnknownColumn { column: ColumnId },
    #[error("column `{name}` changed its {field} since the store was created")]
    ColumnChanged { name: String, field: &'static str },
    #[error("column `{name}` still holds keys and can't be removed")]
    ColumnNotEmpty { name: String },
    #[error("key of {length:?} doesn't fit column {column:?}")]
    KeyWidth { column: ColumnId, length: ByteCount },
    #[error("value of {length:?} is over the limit of {limit:?}")]
    ValueTooLarge { length: ByteCount, limit: ByteCount },
    #[error("batch of {length:?} doesn't fit one segment of {limit:?}")]
    BatchTooLarge { length: ByteCount, limit: ByteCount },
    #[error("batch of {rows:?} doesn't fit one segment of {limit:?}")]
    BatchTooManyRows { rows: RowCount, limit: RowCount },
    #[error("volume {volume:?} holds as many pending range deletes as it can")]
    RangeDeletesFull { volume: VolumeId },
    #[error("{limit} cues are live, which is as many as a store holds")]
    CuesFull { limit: u64 },
    #[error("{limit} scans are live, which is as many as a store holds")]
    ScansFull { limit: u64 },
    #[error("a budget of {budget:?} is under the {least:?} this store needs")]
    BudgetTooSmall { budget: ByteCount, least: ByteCount },
    #[error("{maintenance_workers} maintenance workers and {read_threads} read threads need {least:?}, past a budget of {budget:?}")]
    ThreadsPastBudget { maintenance_workers: usize, read_threads: usize, budget: ByteCount, least: ByteCount },
    #[error("the store runs no read threads, so it answers only the blocking calls")]
    NoAsyncReads,
    #[error("a segment size of {asked:?} is outside the {least:?} to {most:?} a volume may set")]
    SegmentSize { asked: ByteCount, least: ByteCount, most: ByteCount },
    #[error("column {column}: {reason}")]
    BadIndex { column: String, reason: &'static str },
    #[error("batch touches more than one volume")]
    BatchAcrossVolumes,
    #[error("a record in column {column:?} failed its check")]
    Corruption { column: ColumnId },
    #[error("the damage pool of volume {volume:?} is full, so a clear that splits a span has to wait")]
    DamagePoolFull { volume: VolumeId },
    #[error("volume {volume:?} is below its free space reserve")]
    VolumeFull { volume: VolumeId },
    #[error("volume {volume:?} has every table entry in use, so its seal waits for a rewrite to free one")]
    TableFull { volume: VolumeId },
    #[error("volume {volume:?} refuses writes after a failed sync")]
    VolumeFailed { volume: VolumeId },
    #[error("root {root:?} is held by process {holder:?}")]
    Locked { root: PathBuf, holder: ProcessId },
    #[error("store is closed")]
    Closed,
    #[error("store is open read-only")]
    ReadOnly,
    #[error("{action}: {source}")]
    Io {
        action: &'static str,
        source: IoError,
    },
}
