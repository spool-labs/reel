//! The store consumers code against

use crate::column_id::ColumnId;
use crate::maintenance_report::MaintenanceReport;
use crate::memory_report::MemoryReport;
use crate::reads::Reads;
use crate::store_error::StoreError;
use crate::units::{ByteCount, DeathMark, RowCount};
use crate::usage::{ColumnUsage, UsageReport};
use crate::write_batch::WriteBatch;

/// A key-value store shared across threads, where each key behaves as one register
pub trait Store: Reads {
    /// A read view at one point in the write order
    type Cue: Reads;

    /// Write a value, visible to every later read when this returns
    fn put(&self, column: ColumnId, key: &[u8], value: &[u8]) -> Result<(), StoreError>;

    /// Write a value and take its buffer, so a none column never copies it
    fn put_owned(&self, column: ColumnId, key: &[u8], value: Vec<u8>) -> Result<(), StoreError>;

    /// Write a value that dies at a mark, so it sits with records that die near it
    fn put_marked(&self, column: ColumnId, key: &[u8], value: &[u8], mark: DeathMark) -> Result<(), StoreError>;

    /// Write a value that dies at a mark and take its buffer
    fn put_owned_marked(&self, column: ColumnId, key: &[u8], value: Vec<u8>, mark: DeathMark) -> Result<(), StoreError>;

    /// Tell the store the current mark, from which it reads how long each marked record has left
    fn set_current_mark(&self, mark: DeathMark);

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

    /// Fill a report with live and on-disk bytes of one column
    fn column_disk_usage(&self, column: ColumnId, report: &mut ColumnUsage) -> Result<(), StoreError>;

    /// Fill a report with used and free bytes of every volume and root
    fn volume_usage(&self, report: &mut UsageReport) -> Result<(), StoreError>;

    /// Report the heap each structure holds
    fn memory_report(&self) -> MemoryReport;

    /// Fill a report with each job's state, the reclaim counts and the volume readings
    fn maintenance_report(&self, report: &mut MaintenanceReport);

    /// Stop every job, make every returned write durable and release the lock
    fn close(&self) -> Result<(), StoreError>;
}
