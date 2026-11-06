//! The store and cue behind the traits consumers code against

use std::future::Future;

use reel_api::column_id::ColumnId;
use reel_api::direction::Direction;
use reel_api::maintenance_report::MaintenanceReport;
use reel_api::memory_report::MemoryReport;
use reel_api::reads::{Reads, ValueWindow};
use reel_api::scan::{KeyScan, Scan, ScanStep};
use reel_api::store::Store;
use reel_api::store_error::StoreError;
use reel_api::units::{ByteCount, DeathMark, RowCount};
use reel_api::usage::{ColumnUsage, UsageReport};
use reel_api::value_list::ValueList;
use reel_api::write_batch::WriteBatch;

use crate::engine::cue::Cue;
use crate::engine::read_path::RecordWant;
use crate::engine::reel_store::ReelStore;
use crate::engine::store_scan::StoreScan;
use crate::io::Io;

impl<Backend: Io> Scan for StoreScan<Backend> {
    fn next_row(&mut self, key: &mut Vec<u8>, value: &mut Vec<u8>) -> Result<ScanStep, StoreError> {
        StoreScan::next_row(self, key, value)
    }

    fn next_block(&mut self, keys: &mut ValueList) -> impl Future<Output = Result<bool, StoreError>> + Send {
        StoreScan::next_block(self, keys)
    }
}

impl<Backend: Io> KeyScan for StoreScan<Backend> {
    fn next_key(&mut self, key: &mut Vec<u8>) -> Result<ScanStep, StoreError> {
        StoreScan::next_key(self, key)
    }

    fn next_key_block(&mut self, keys: &mut ValueList) -> impl Future<Output = Result<bool, StoreError>> + Send {
        StoreScan::next_block(self, keys)
    }
}

impl<Backend: Io> Reads for ReelStore<Backend> {
    type Rows = StoreScan<Backend>;
    type Keys = StoreScan<Backend>;

    fn get(&self, column: ColumnId, key: &[u8], buf: &mut Vec<u8>) -> Result<bool, StoreError> {
        ReelStore::get(self, column, key, buf)
    }

    fn get_many(&self, column: ColumnId, keys: &[&[u8]], values: &mut ValueList) -> Result<(), StoreError> {
        ReelStore::get_many(self, column, keys, values)
    }

    fn get_range(&self, column: ColumnId, key: &[u8], window: ValueWindow, buf: &mut Vec<u8>) -> Result<bool, StoreError> {
        ReelStore::get_range(self, column, key, window, buf)
    }

    fn contains(&self, column: ColumnId, key: &[u8]) -> Result<bool, StoreError> {
        ReelStore::contains(self, column, key)
    }

    fn iter_from(&self, column: ColumnId, start: &[u8], direction: Direction) -> Result<Self::Rows, StoreError> {
        ReelStore::iter_from(self, column, start, direction)
    }

    fn iter_range(&self, column: ColumnId, start: &[u8], end: &[u8], direction: Direction) -> Result<Self::Rows, StoreError> {
        ReelStore::iter_range(self, column, start, end, direction)
    }

    fn iter_prefix(&self, column: ColumnId, prefix: &[u8]) -> Result<Self::Rows, StoreError> {
        ReelStore::iter_prefix(self, column, prefix)
    }

    fn iter_keys_from(&self, column: ColumnId, start: &[u8], direction: Direction) -> Result<Self::Keys, StoreError> {
        ReelStore::iter_from(self, column, start, direction)
    }

    fn iter_keys_range(&self, column: ColumnId, start: &[u8], end: &[u8], direction: Direction) -> Result<Self::Keys, StoreError> {
        ReelStore::iter_range(self, column, start, end, direction)
    }

    fn iter_keys_prefix(&self, column: ColumnId, prefix: &[u8]) -> Result<Self::Keys, StoreError> {
        ReelStore::iter_prefix(self, column, prefix)
    }

    fn count_prefix(&self, column: ColumnId, prefix: &[u8]) -> Result<RowCount, StoreError> {
        ReelStore::count_prefix(self, column, prefix)
    }

    fn bytes_prefix(&self, column: ColumnId, prefix: &[u8]) -> Result<ByteCount, StoreError> {
        ReelStore::bytes_prefix(self, column, prefix)
    }

    fn get_async(&self, column: ColumnId, key: &[u8], buf: &mut Vec<u8>) -> impl Future<Output = Result<bool, StoreError>> + Send {
        self.state.read_async(column, key, RecordWant::Whole, None, buf)
    }

    async fn get_many_async(&self, column: ColumnId, keys: &[&[u8]], values: &mut ValueList) -> Result<(), StoreError> {
        let cue = ReelStore::cue(self)?;
        cue.many_async(column, keys, values).await
    }

    fn get_range_async(
        &self,
        column: ColumnId,
        key: &[u8],
        window: ValueWindow,
        buf: &mut Vec<u8>,
    ) -> impl Future<Output = Result<bool, StoreError>> + Send {
        self.state.read_async(column, key, RecordWant::Window(window), None, buf)
    }
}

impl<Backend: Io> Reads for Cue<Backend> {
    type Rows = StoreScan<Backend>;
    type Keys = StoreScan<Backend>;

    fn get(&self, column: ColumnId, key: &[u8], buf: &mut Vec<u8>) -> Result<bool, StoreError> {
        Cue::get(self, column, key, buf)
    }

    fn get_many(&self, column: ColumnId, keys: &[&[u8]], values: &mut ValueList) -> Result<(), StoreError> {
        Cue::get_many(self, column, keys, values)
    }

    fn get_range(&self, column: ColumnId, key: &[u8], window: ValueWindow, buf: &mut Vec<u8>) -> Result<bool, StoreError> {
        Cue::get_range(self, column, key, window, buf)
    }

    fn contains(&self, column: ColumnId, key: &[u8]) -> Result<bool, StoreError> {
        Cue::contains(self, column, key)
    }

    fn iter_from(&self, column: ColumnId, start: &[u8], direction: Direction) -> Result<Self::Rows, StoreError> {
        Cue::iter_from(self, column, start, direction)
    }

    fn iter_range(&self, column: ColumnId, start: &[u8], end: &[u8], direction: Direction) -> Result<Self::Rows, StoreError> {
        Cue::iter_range(self, column, start, end, direction)
    }

    fn iter_prefix(&self, column: ColumnId, prefix: &[u8]) -> Result<Self::Rows, StoreError> {
        Cue::iter_prefix(self, column, prefix)
    }

    fn iter_keys_from(&self, column: ColumnId, start: &[u8], direction: Direction) -> Result<Self::Keys, StoreError> {
        Cue::iter_from(self, column, start, direction)
    }

    fn iter_keys_range(&self, column: ColumnId, start: &[u8], end: &[u8], direction: Direction) -> Result<Self::Keys, StoreError> {
        Cue::iter_range(self, column, start, end, direction)
    }

    fn iter_keys_prefix(&self, column: ColumnId, prefix: &[u8]) -> Result<Self::Keys, StoreError> {
        Cue::iter_prefix(self, column, prefix)
    }

    fn count_prefix(&self, column: ColumnId, prefix: &[u8]) -> Result<RowCount, StoreError> {
        Cue::count_prefix(self, column, prefix)
    }

    fn bytes_prefix(&self, column: ColumnId, prefix: &[u8]) -> Result<ByteCount, StoreError> {
        Cue::bytes_prefix(self, column, prefix)
    }

    fn get_async(&self, column: ColumnId, key: &[u8], buf: &mut Vec<u8>) -> impl Future<Output = Result<bool, StoreError>> + Send {
        self.read_async(column, key, RecordWant::Whole, buf)
    }

    fn get_many_async(
        &self,
        column: ColumnId,
        keys: &[&[u8]],
        values: &mut ValueList,
    ) -> impl Future<Output = Result<(), StoreError>> + Send {
        self.many_async(column, keys, values)
    }

    fn get_range_async(
        &self,
        column: ColumnId,
        key: &[u8],
        window: ValueWindow,
        buf: &mut Vec<u8>,
    ) -> impl Future<Output = Result<bool, StoreError>> + Send {
        self.read_async(column, key, RecordWant::Window(window), buf)
    }
}

impl<Backend: Io> Store for ReelStore<Backend> {
    type Cue = Cue<Backend>;

    fn put(&self, column: ColumnId, key: &[u8], value: &[u8]) -> Result<(), StoreError> {
        ReelStore::put(self, column, key, value)
    }

    fn put_owned(&self, column: ColumnId, key: &[u8], value: Vec<u8>) -> Result<(), StoreError> {
        ReelStore::put_owned(self, column, key, value)
    }

    fn put_marked(&self, column: ColumnId, key: &[u8], value: &[u8], mark: DeathMark) -> Result<(), StoreError> {
        ReelStore::put_marked(self, column, key, value, mark)
    }

    fn put_owned_marked(&self, column: ColumnId, key: &[u8], value: Vec<u8>, mark: DeathMark) -> Result<(), StoreError> {
        ReelStore::put_owned_marked(self, column, key, value, mark)
    }

    fn set_current_mark(&self, mark: DeathMark) {
        ReelStore::set_current_mark(self, mark);
    }

    fn delete(&self, column: ColumnId, key: &[u8]) -> Result<(), StoreError> {
        ReelStore::delete(self, column, key)
    }

    fn delete_range(&self, column: ColumnId, start: &[u8], end: &[u8]) -> Result<(), StoreError> {
        ReelStore::delete_range(self, column, start, end)
    }

    fn write_batch(&self, batch: &WriteBatch) -> Result<(), StoreError> {
        ReelStore::write_batch(self, batch)
    }

    fn flush(&self) -> Result<(), StoreError> {
        ReelStore::flush(self)
    }

    fn cue(&self) -> Result<Self::Cue, StoreError> {
        ReelStore::cue(self)
    }

    fn live_data_size_bytes(&self) -> ByteCount {
        ReelStore::live_data_size_bytes(self)
    }

    fn key_count_estimate(&self) -> RowCount {
        ReelStore::key_count_estimate(self)
    }

    fn column_disk_usage(&self, column: ColumnId, report: &mut ColumnUsage) -> Result<(), StoreError> {
        ReelStore::column_disk_usage(self, column, report)
    }

    fn volume_usage(&self, report: &mut UsageReport) -> Result<(), StoreError> {
        ReelStore::volume_usage(self, report)
    }

    fn memory_report(&self) -> MemoryReport {
        ReelStore::memory_report(self)
    }

    fn maintenance_report(&self, report: &mut MaintenanceReport) {
        ReelStore::maintenance_report(self, report);
    }

    fn close(&self) -> Result<(), StoreError> {
        ReelStore::close(self)
    }
}
