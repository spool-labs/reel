//! The store consumers code against

use crate::column_id::ColumnId;
use crate::maintenance_report::MaintenanceReport;
use crate::memory_report::MemoryReport;
use crate::reads::Reads;
use crate::store_error::StoreError;
use crate::units::{ByteCount, RowCount};
use crate::usage::{ColumnUsage, VolumeUsage};
use crate::write_batch::WriteBatch;

/// A key-value store shared across threads, where each key behaves as one register
pub trait Store: Reads {
    /// A read view at one point in the write order
    type Cue: Reads;

    /// Write a value, visible to every later read when this returns
    fn put(&self, column: ColumnId, key: &[u8], value: &[u8]) -> Result<(), StoreError>;

    /// Write a value and take its buffer, so a none column never copies it
    fn put_owned(&self, column: ColumnId, key: &[u8], value: Vec<u8>) -> Result<(), StoreError>;

    /// Write a grave without reading any device
    fn delete(&self, column: ColumnId, key: &[u8]) -> Result<(), StoreError>;

    /// Remove every key in [start, end) written before this call
    fn delete_range(&self, column: ColumnId, start: &[u8], end: &[u8]) -> Result<(), StoreError>;

    /// Apply a batch whole, in batch order
    fn write_batch(&self, batch: &WriteBatch) -> Result<(), StoreError>;

    /// Make every write that returned before this call durable on every volume
    fn flush(&self) -> Result<(), StoreError>;

    /// Take a read view that copies no data and never blocks a writer
    fn cue(&self) -> Result<Self::Cue, StoreError>;

    /// Estimate the live bytes of the whole store, without a scan
    fn live_data_size_bytes(&self) -> ByteCount;

    /// Estimate the keys of the whole store, without a scan
    fn key_count_estimate(&self) -> RowCount;

    /// Report live and on-disk bytes of one column
    fn column_disk_usage(&self, column: ColumnId) -> Result<ColumnUsage, StoreError>;

    /// Report used and free bytes of every volume and root
    fn volume_usage(&self) -> Result<Vec<VolumeUsage>, StoreError>;

    /// Report the heap each structure holds
    fn memory_report(&self) -> MemoryReport;

    /// Report each job's state and the reclaim counts
    fn maintenance_report(&self) -> MaintenanceReport;

    /// Stop every job, make every returned write durable and release the lock
    fn close(&self) -> Result<(), StoreError>;
}
