//! Core storage trait defining the key-value store interface

use crate::value::Value;
use std::future::Future;
use std::path::Path;

use crate::{Error, Result, WriteBatch};

/// Iterator direction for scanning (lexicographic order)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    /// Ascending order (smallest to largest)
    Asc,
    /// Descending order (largest to smallest)
    Desc,
}

/// Key-value pair type returned by iterators
///
/// The value carries its buffer's owner, so a backend that read several records
/// into one buffer can lend a window into it rather than cut it into vectors.
pub type KeyValue = (Vec<u8>, Value);

/// Boxed iterator type for store operations
pub type StoreIter<'a> = Box<dyn Iterator<Item = KeyValue> + 'a>;

/// Role of a physical storage volume.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreVolume {
    /// The metadata/index volume, or the whole store when not split.
    Primary,
    /// The bulk volume for large payloads.
    Bulk,
}

/// Best-effort disk usage for one physical storage volume.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiskVolume {
    pub volume: StoreVolume,
    pub used_bytes: u64,
    pub free_bytes: Option<u64>,
}

/// Best-effort on-disk usage for one column family, tagged with its volume.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CfDiskUsage {
    /// Column family name.
    pub cf: String,
    /// Physical volume the column lives on.
    pub volume: StoreVolume,
    /// Bytes held in SST files.
    pub sst_bytes: u64,
    /// Bytes held in blob files, zero for columns that store values inline.
    pub blob_bytes: u64,
    /// Estimated live key count.
    pub num_keys: u64,
}

impl CfDiskUsage {
    /// Total on-disk bytes for the column family (SST plus blob files).
    pub fn total_bytes(&self) -> u64 {
        self.sst_bytes.saturating_add(self.blob_bytes)
    }
}

/// One page of a sweep and where the next one starts
pub type SweptPage = (Vec<(Vec<u8>, Value)>, Option<Vec<u8>>);

/// Trait for key-value storage with column family support
///
/// Each column family is its own key space within one store.
pub trait Store: Send + Sync {
    /// Get a value by key from the specified column family.
    fn get(&self, cf: &str, key: &[u8]) -> Result<Option<Value>>;

    /// Get several values from one column family, answered in the order asked.
    ///
    /// The default asks one at a time; a backend overrides to put them all in
    /// front of its device at once.
    fn get_many(&self, cf: &str, keys: &[&[u8]]) -> Result<Vec<Option<Value>>> {
        keys.iter().map(|key| self.get(cf, key)).collect()
    }

    /// Get a value by key, awaited rather than waited for on the calling thread.
    ///
    /// Not dispatchable through `dyn Store`, since the future's type is the
    /// backend's own. The default answers from the blocking call.
    fn get_wait(&self, cf: &str, key: &[u8]) -> impl Future<Output = Result<Option<Value>>> + Send
    where
        Self: Sized,
    {
        std::future::ready(self.get(cf, key))
    }

    /// Get part of one value, from `offset` for `len` bytes.
    ///
    /// The range is clamped as a `pread` is: one reaching past the end answers the
    /// bytes that are there, one starting past it answers none, and a missing key
    /// answers nothing at all.
    fn get_range(&self, cf: &str, key: &[u8], offset: u64, len: usize) -> Result<Option<Value>> {
        Ok(self.get(cf, key)?.map(|value| range_of(value, offset, len)))
    }

    /// Get part of one value, awaited rather than waited for on the calling thread.
    fn get_range_wait(
        &self,
        cf: &str,
        key: &[u8],
        offset: u64,
        len: usize,
    ) -> impl Future<Output = Result<Option<Value>>> + Send
    where
        Self: Sized,
    {
        std::future::ready(self.get_range(cf, key, offset, len))
    }

    /// Get several values from one column family, awaited, answered in order.
    fn get_many_wait(
        &self,
        cf: &str,
        keys: &[&[u8]],
    ) -> impl Future<Output = Result<Vec<Option<Value>>>> + Send
    where
        Self: Sized,
    {
        std::future::ready(self.get_many(cf, keys))
    }

    /// Put a key-value pair into the specified column family.
    fn put(&self, cf: &str, key: &[u8], value: &[u8]) -> Result<()>;

    /// Put a key-value pair, awaited rather than waited for on the calling thread.
    ///
    /// Not dispatchable through `dyn Store`, for the same reason `get_wait` is not.
    fn put_wait(
        &self,
        cf: &str,
        key: &[u8],
        value: &[u8],
    ) -> impl Future<Output = Result<()>> + Send
    where
        Self: Sized,
    {
        std::future::ready(self.put(cf, key, value))
    }

    /// Delete a key from the specified column family.
    fn delete(&self, cf: &str, key: &[u8]) -> Result<()>;

    /// Check if a key exists in the specified column family.
    fn contains(&self, cf: &str, key: &[u8]) -> Result<bool>;

    /// Apply a batch of write operations atomically.
    ///
    /// Atomicity holds only within a single backend. A backend split across
    /// independent instances may write a cross-instance batch non-atomically.
    fn write_batch(&self, batch: WriteBatch) -> Result<()>;

    /// Apply a batch of write operations atomically, awaited.
    ///
    /// One durability point for the whole batch, however many keys it carries.
    fn write_batch_wait(&self, batch: WriteBatch) -> impl Future<Output = Result<()>> + Send
    where
        Self: Sized,
    {
        std::future::ready(self.write_batch(batch))
    }

    /// Delete every key in the range `[start, end)` from the column family.
    /// Backends can override with a native range tombstone; the default collects
    /// the keys in range and deletes them in one batch.
    fn delete_range(&self, cf: &str, start: &[u8], end: &[u8]) -> Result<()> {
        let keys: Vec<Vec<u8>> = self.iter_range(cf, start, end)?.map(|(k, _)| k).collect();
        if keys.is_empty() {
            return Ok(());
        }
        // The family is whatever the caller named, not a column constant, so it
        // is carried owned.
        let mut batch = WriteBatch::new();
        for key in keys {
            batch.delete_named(cf.to_string().into(), key);
        }
        self.write_batch(batch)
    }

    /// Iterate over all entries in lexicographic key order.
    fn iter(&self, cf: &str) -> Result<StoreIter<'_>>;

    /// Iterate over entries matching the key prefix in lexicographic order.
    fn iter_prefix(&self, cf: &str, prefix: &[u8]) -> Result<StoreIter<'_>>;

    /// Collect the keys under `prefix` WITHOUT reading their values. Backends can
    /// override to skip value (e.g. blob-file) reads when only keys are needed.
    fn iter_keys_prefix(&self, cf: &str, prefix: &[u8]) -> Result<Vec<Vec<u8>>> {
        Ok(self.iter_prefix(cf, prefix)?.map(|(k, _)| k).collect())
    }

    /// One page of a column family, resumable by an opaque mark.
    ///
    /// Promises only that a full sweep hands out every live key at least once,
    /// in whatever order the backend keeps. `None` back means the family is
    /// done. The default walks in key order and marks with the last key handed
    /// out, which is what an ordered backend wants; a backend whose keys have no
    /// order overrides it and marks in its own terms.
    fn sweep(&self, cf: &str, from: Option<&[u8]>, limit: usize) -> Result<SweptPage> {
        // The mark is where to resume, inclusive, so it is the first key this
        // page did not return and the next page starts on it. Skipping it here
        // would drop one key per page boundary.
        let start = from.unwrap_or(&[]);
        let mut rows = Vec::with_capacity(limit);
        let mut next = None;
        for (key, value) in self.iter_from(cf, start, Direction::Asc)? {
            if rows.len() == limit {
                next = Some(key);
                break;
            }
            rows.push((key, value));
        }
        Ok((rows, next))
    }

    /// One page of the keys under a prefix, resumable by an opaque mark.
    ///
    /// Same promise as `sweep`, narrowed to a prefix. A backend whose keys have
    /// no order can serve this only where the prefix selects a whole shard of
    /// its own, and answers nothing where it does not, so a caller cannot turn a
    /// prefix walk into a scan of the family by accident. The default walks the
    /// prefix in key order, which an ordered backend can always do.
    fn sweep_prefix(
        &self,
        cf: &str,
        prefix: &[u8],
        from: Option<&[u8]>,
        limit: usize,
    ) -> Result<SweptPage> {
        // Inclusive, as in `sweep`: the mark is the first key not returned.
        let start = from.unwrap_or(prefix);
        let mut rows = Vec::with_capacity(limit);
        let mut next = None;
        for (key, value) in self.iter_from(cf, start, Direction::Asc)? {
            if !key.starts_with(prefix) {
                break;
            }
            if rows.len() == limit {
                next = Some(key);
                break;
            }
            rows.push((key, value));
        }
        Ok((rows, next))
    }

    /// Exact count of the keys under `prefix`, WITHOUT materializing them.
    ///
    /// The default collects the keys and takes the length; backends override to
    /// count in place.
    fn count_prefix(&self, cf: &str, prefix: &[u8]) -> Result<u64> {
        Ok(self.iter_keys_prefix(cf, prefix)?.len() as u64)
    }

    /// Stored value bytes under `prefix`, WITHOUT reading any of them.
    ///
    /// The byte twin of `count_prefix`, and stored bytes rather than anything the
    /// caller put in: whatever the backend holds under those keys. Nothing comes
    /// back from a backend that can only answer by reading payloads. Has no
    /// default, so a delegating store cannot inherit a no-answer silently.
    fn bytes_prefix(&self, cf: &str, prefix: &[u8]) -> Result<Option<u64>>;

    /// Iterate from the start key (inclusive) in the specified direction.
    fn iter_from(&self, cf: &str, start: &[u8], direction: Direction) -> Result<StoreIter<'_>>;

    /// Walk from the start key, lending each row to the callback
    ///
    /// The callback returns false to stop the walk. `hint` is how many rows the
    /// caller expects to take, zero for no estimate.
    fn walk_from(
        &self,
        cf: &str,
        start: &[u8],
        direction: Direction,
        hint: usize,
        take: &mut dyn FnMut(&[u8], &[u8]) -> bool,
    ) -> Result<()> {
        let _ = hint;
        for (key, value) in self.iter_from(cf, start, direction)? {
            if !take(&key, &value) {
                break;
            }
        }
        Ok(())
    }

    /// Iterate over entries in the key range [start, end) in lexicographic order.
    fn iter_range(&self, cf: &str, start: &[u8], end: &[u8]) -> Result<StoreIter<'_>>;

    /// Best-effort total backend size in bytes, overhead included.
    fn actual_size_bytes(&self) -> Result<u64> {
        Ok(0)
    }

    /// Best-effort free disk space, nothing for a backend without a filesystem.
    fn available_disk_bytes(&self) -> Result<Option<u64>> {
        Ok(None)
    }

    /// Cheap on-disk footprint of persisted live data, safe to poll on every
    /// scrape. Must not walk the filesystem; a backend that cannot answer
    /// cheaply returns nothing. Has no default, so a delegating store cannot
    /// inherit a no-answer silently.
    fn live_data_size_bytes(&self) -> Result<Option<u64>>;

    /// Cheap approximate key count for a named column family, safe to poll.
    /// Has no default, for the same reason.
    fn key_count_estimate(&self, cf: &str) -> Result<Option<u64>>;

    /// Best-effort on-disk usage per column family, empty when it cannot be
    /// answered cheaply.
    fn cf_disk_usage(&self) -> Result<Vec<CfDiskUsage>> {
        Ok(Vec::new())
    }

    /// Best-effort background space reclamation, a no-op where unsupported.
    fn reclaim_space(&self) -> Result<()> {
        Ok(())
    }

    /// Best-effort bounded pass of background upkeep, driven on a timer.
    ///
    /// One bounded unit of work per call, so the caller sets the cadence. A
    /// backend that keeps its own threads no-ops here.
    fn maintain(&self) -> Result<()> {
        Ok(())
    }

    /// Best-effort disk usage per physical volume, one entry per device.
    fn disk_volumes(&self) -> Result<Vec<DiskVolume>> {
        Ok(vec![DiskVolume {
            volume: StoreVolume::Primary,
            used_bytes: self.actual_size_bytes()?,
            free_bytes: self.available_disk_bytes()?,
        }])
    }
}

/// The part of a value a ranged read asked for, clamped to what the value holds
///
/// Both ends clamp rather than refuse, so a caller stepping a value by fixed
/// windows walks off the end without a special case.
pub fn range_of(value: Value, offset: u64, len: usize) -> Value {
    let held = value.len();
    let at = offset.min(held as u64) as usize;
    let end = at.saturating_add(len).min(held);
    Value::new(value[at..end].to_vec())
}

/// Total bytes of every file under a directory tree, zero when it is absent
pub fn directory_size_bytes(path: &Path) -> Result<u64> {
    let entries = match std::fs::read_dir(path) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(error) => return Err(Error::Io(error)),
    };

    let mut total = 0u64;
    for entry in entries {
        let entry = entry.map_err(Error::Io)?;
        let file_type = entry.file_type().map_err(Error::Io)?;
        if file_type.is_dir() {
            total = total.saturating_add(directory_size_bytes(&entry.path())?);
        } else if file_type.is_file() {
            total = total.saturating_add(entry.metadata().map_err(Error::Io)?.len());
        }
    }

    Ok(total)
}
