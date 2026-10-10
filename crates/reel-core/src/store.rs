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
pub type KeyValue = (Vec<u8>, Value);

/// Boxed iterator type for store operations
pub type StoreIter<'a> = Box<dyn Iterator<Item = KeyValue> + 'a>;

/// Role of a physical storage volume
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreVolume {
    /// The metadata/index volume, or the whole store when not split
    Primary,
    /// The bulk volume for large payloads
    Bulk,
}

/// Best-effort disk usage for one physical storage volume
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiskVolume {
    pub volume: StoreVolume,
    pub used_bytes: u64,
    pub free_bytes: Option<u64>,
}

/// Best-effort on-disk usage for one column family, tagged with its volume
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CfDiskUsage {
    /// The column family's name
    pub cf: String,
    /// The physical volume holding the column
    pub volume: StoreVolume,
    /// Bytes held in SST files
    pub sst_bytes: u64,
    /// Bytes held in blob files, zero for columns that store values inline
    pub blob_bytes: u64,
    /// Estimated live key count
    pub num_keys: u64,
}

impl CfDiskUsage {
    /// Total on-disk bytes for the column family (SST plus blob files)
    pub fn total_bytes(&self) -> u64 {
        self.sst_bytes.saturating_add(self.blob_bytes)
    }
}

/// One page of a sweep and where the next one starts
pub type SweptPage = (Vec<(Vec<u8>, Value)>, Option<Vec<u8>>);

/// One page of swept keys and where the next one starts
pub type SweptKeys = (Vec<Vec<u8>>, Option<Vec<u8>>);

/// Trait for key-value storage with column family support
pub trait Store: Send + Sync {
    /// Get a value by key from the specified column family
    fn get(&self, cf: &str, key: &[u8]) -> Result<Option<Value>>;

    /// Get several values from one column family, answered in the order asked
    fn get_many(&self, cf: &str, keys: &[&[u8]]) -> Result<Vec<Option<Value>>> {
        keys.iter().map(|key| self.get(cf, key)).collect()
    }

    /// Get a value by key without blocking the calling thread
    fn get_wait(&self, cf: &str, key: &[u8]) -> impl Future<Output = Result<Option<Value>>> + Send
    where
        Self: Sized,
    {
        std::future::ready(self.get(cf, key))
    }

    /// Get `len` bytes of one value from `offset`, clamped to the value's end
    fn get_range(&self, cf: &str, key: &[u8], offset: u64, len: usize) -> Result<Option<Value>> {
        Ok(self.get(cf, key)?.map(|value| range_of(value, offset, len)))
    }

    /// Get part of one value without blocking the calling thread
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

    /// Get several values from one column family, awaited, answered in order
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

    /// Put a key-value pair into the specified column family
    fn put(&self, cf: &str, key: &[u8], value: &[u8]) -> Result<()>;

    /// Put a key-value pair without blocking the calling thread
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

    /// Delete a key from the specified column family
    fn delete(&self, cf: &str, key: &[u8]) -> Result<()>;

    /// Check if a key exists in the specified column family
    fn contains(&self, cf: &str, key: &[u8]) -> Result<bool>;

    /// Apply a batch of write operations, atomically within a single backend
    fn write_batch(&self, batch: WriteBatch) -> Result<()>;

    /// Apply a batch of write operations atomically, awaited, with one durability point
    fn write_batch_wait(&self, batch: WriteBatch) -> impl Future<Output = Result<()>> + Send
    where
        Self: Sized,
    {
        std::future::ready(self.write_batch(batch))
    }

    /// Delete every key in the range `[start, end)` from the column family
    fn delete_range(&self, cf: &str, start: &[u8], end: &[u8]) -> Result<()> {
        let keys: Vec<Vec<u8>> = self.iter_range(cf, start, end)?.map(|(k, _)| k).collect();
        if keys.is_empty() {
            return Ok(());
        }
        // The caller's family is a runtime string, so the batch takes it owned
        let mut batch = WriteBatch::new();
        for key in keys {
            batch.delete_named(cf.to_string().into(), key);
        }
        self.write_batch(batch)
    }

    /// Iterate over all entries in lexicographic key order
    fn iter(&self, cf: &str) -> Result<StoreIter<'_>>;

    /// Iterate over entries matching the key prefix in lexicographic order
    fn iter_prefix(&self, cf: &str, prefix: &[u8]) -> Result<StoreIter<'_>>;

    /// Collect the keys under `prefix`, which a backend can do without reading values
    fn iter_keys_prefix(&self, cf: &str, prefix: &[u8]) -> Result<Vec<Vec<u8>>> {
        Ok(self.iter_prefix(cf, prefix)?.map(|(k, _)| k).collect())
    }

    /// One page of a column family, resumable by an opaque mark, `None` once the family is done
    fn sweep(&self, cf: &str, from: Option<&[u8]>, limit: usize) -> Result<SweptPage> {
        // The mark is inclusive, the first key this page did not return
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

    /// One page of the keys under a prefix, resumable by an opaque mark
    fn sweep_prefix(
        &self,
        cf: &str,
        prefix: &[u8],
        from: Option<&[u8]>,
        limit: usize,
    ) -> Result<SweptPage> {
        // The mark is inclusive, as in `sweep`
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

    /// One page of just the keys under a prefix, resumable by an opaque mark
    fn sweep_keys_prefix(
        &self,
        cf: &str,
        prefix: &[u8],
        from: Option<&[u8]>,
        limit: usize,
    ) -> Result<SweptKeys> {
        let (rows, next) = self.sweep_prefix(cf, prefix, from, limit)?;
        Ok((rows.into_iter().map(|(key, _)| key).collect(), next))
    }

    /// Exact count of the keys under `prefix`, which a backend can count in place
    fn count_prefix(&self, cf: &str, prefix: &[u8]) -> Result<u64> {
        Ok(self.iter_keys_prefix(cf, prefix)?.len() as u64)
    }

    /// Stored value bytes under `prefix` without reading them, or nothing if the backend cannot
    fn bytes_prefix(&self, cf: &str, prefix: &[u8]) -> Result<Option<u64>>;

    /// Iterate from the start key (inclusive) in the specified direction
    fn iter_from(&self, cf: &str, start: &[u8], direction: Direction) -> Result<StoreIter<'_>>;

    /// Lend rows from `start` to `take` until it returns false, `hint` rows expected or zero
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

    /// Iterate over entries in the key range `[start, end)` in lexicographic order
    fn iter_range(&self, cf: &str, start: &[u8], end: &[u8]) -> Result<StoreIter<'_>>;

    /// Best-effort total backend size in bytes, overhead included
    fn actual_size_bytes(&self) -> Result<u64> {
        Ok(0)
    }

    /// Best-effort free disk space, nothing for a backend without a filesystem
    fn available_disk_bytes(&self) -> Result<Option<u64>> {
        Ok(None)
    }

    /// Cheap on-disk size of live data, safe to poll, or nothing when it cannot be had cheaply
    fn live_data_size_bytes(&self) -> Result<Option<u64>>;

    /// Cheap approximate key count for a column family, safe to poll
    fn key_count_estimate(&self, cf: &str) -> Result<Option<u64>>;

    /// Best-effort on-disk usage per column family, empty when it cannot be had cheaply
    fn cf_disk_usage(&self) -> Result<Vec<CfDiskUsage>> {
        Ok(Vec::new())
    }

    /// Best-effort background space reclamation, a no-op where unsupported
    fn reclaim_space(&self) -> Result<()> {
        Ok(())
    }

    /// Best-effort bounded pass of background upkeep, driven on a timer
    fn maintain(&self) -> Result<()> {
        Ok(())
    }

    /// Best-effort disk usage per physical volume, one entry per device
    fn disk_volumes(&self) -> Result<Vec<DiskVolume>> {
        Ok(vec![DiskVolume {
            volume: StoreVolume::Primary,
            used_bytes: self.actual_size_bytes()?,
            free_bytes: self.available_disk_bytes()?,
        }])
    }
}

/// The part of a value a ranged read asked for, clamped to what the value holds
pub fn range_of(value: Value, offset: u64, len: usize) -> Value {
    let held = value.len();
    let at = offset.min(held as u64) as usize;
    let end = at.saturating_add(len).min(held);
    Value::new(value[at..end].to_vec())
}

/// Bytes a file takes on disk, which for a sparse file is less than its length
fn file_bytes(metadata: &std::fs::Metadata) -> u64 {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        metadata.blocks().saturating_mul(512)
    }
    #[cfg(not(unix))]
    {
        metadata.len()
    }
}

/// Total on-disk bytes of every file under a directory tree, zero when it is absent
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
            total = total.saturating_add(file_bytes(&entry.metadata().map_err(Error::Io)?));
        }
    }

    Ok(total)
}
