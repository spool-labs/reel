//! Every way a store call can fail

use std::io::Error as IoError;
use std::path::PathBuf;

use thiserror::Error;

use crate::column_id::ColumnId;
use crate::units::{ByteCount, ProcessId, VolumeId};

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
    #[error("batch touches more than one volume")]
    BatchAcrossVolumes,
    #[error("record of key {key:?} in column {column:?} failed its check")]
    Corruption { column: ColumnId, key: Vec<u8> },
    #[error("volume {volume:?} is below its free space reserve")]
    VolumeFull { volume: VolumeId },
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
