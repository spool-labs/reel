//! The store trait implementation over the reel engine
//!
//! The reel serves the columns it was opened with and rejects every other family.
//! A value is stored exactly as it arrives and read back verbatim, and a playback
//! steps the column's index in key order and reads payloads lazily.

use std::collections::VecDeque;
use std::ops::Bound;
use std::path::Path;
use std::sync::Arc;

use reel_core::store::SweptPage;
use reel_core::{
    directory_size_bytes, BatchOp, CfDiskUsage, Direction, DiskVolume, Error as StoreError,
    Result as StoreResult, Store, StoreIter, StoreVolume, Value, WriteBatch,
};

use crate::engine::{RecordWrite, ReelStore};
use crate::format::column::{ColumnId, KeyRef, KeyWidth, RecordKey, MAX_KEY_LEN};
use crate::index::entry::Entry;
use crate::index::page::KeyPage;
use crate::index::playback::{PlaybackCursor, Way};

/// Keys a playback's first trip to the index pulls
///
/// A caller paging wants its page and nothing more, so the first trip is small. A
/// caller walking a whole column wants few trips, so each trip doubles.
const PLAYBACK_PAGE_MIN: usize = 32;

/// Ceiling the page size stops doubling at
const PLAYBACK_PAGE_MAX: usize = 8192;

/// Records a playback's first run asks the device for
///
/// A caller that stops after the keys it wanted still pays for every payload the run
/// read ahead of it, so the first run is small and each one after it doubles.
const PLAYBACK_RUN_MIN: usize = 8;

/// Ceiling the run stops doubling at
///
/// A page of keys costs one trip to the index, but each payload behind them is a
/// round trip the thread spends waiting, so a run of them is what fills the queue.
const PLAYBACK_RUN_MAX: usize = 128;

/// Payload bytes a playback lets a run reach before it stops adding to it
///
/// Depth is worth having on small records, which are almost all wait, and worth
/// nothing on large ones, which already keep the device busy on their own.
const PLAYBACK_READ_BYTES: u64 = 4 * 1024 * 1024;

thread_local! {
    /// The keys one thread's batched asks are resolved into, kept between batches
    ///
    /// A caller of the store trait hands over borrowed bytes and the engine addresses
    /// records by a key of its own, so every batch builds one list of them. It goes
    /// nowhere, so it stays with the thread.
    static BATCH_KEYS: std::cell::Cell<Vec<RecordKey>> = const { std::cell::Cell::new(Vec::new()) };
}

/// This thread's key list, given back however the batch that took it ends
struct HeldKeys(Vec<RecordKey>);

impl HeldKeys {
    fn take() -> HeldKeys {
        HeldKeys(BATCH_KEYS.with(std::cell::Cell::take))
    }
}

impl Drop for HeldKeys {
    fn drop(&mut self) {
        let mut held = std::mem::take(&mut self.0);
        // Cleared here rather than on the way out, so a wide key's bytes are released
        // when the batch ends rather than held until this thread asks again.
        held.clear();
        BATCH_KEYS.with(|spare| spare.set(held));
    }
}

/// Where one key sits relative to a playback's bounds
#[derive(Clone, Copy, Eq, PartialEq)]
enum Position {
    /// Behind the playback, so skip it and carry on
    Before,

    /// Inside the playback
    Inside,

    /// Past the playback, so this playback is finished
    Past,
}

impl Store for ReelStore {
    fn get(&self, cf: &str, key: &[u8]) -> StoreResult<Option<Value>> {
        let column = self.classify(cf)?;
        let key = self.record_key(column, key)?;
        self.get(&key).map_err(StoreError::from)
    }

    /// Ask the device for every key at once rather than one after another
    fn get_many(&self, cf: &str, keys: &[&[u8]]) -> StoreResult<Vec<Option<Value>>> {
        let column = self.classify(cf)?;
        let mut held = HeldKeys::take();
        let record_keys = &mut held.0;
        for key in keys {
            record_keys.push(self.record_key(column, key)?);
        }
        self.get_many(record_keys).map_err(StoreError::from)
    }

    /// Read a window of one value, moving only the bytes the window covers
    ///
    /// The index places the payload, so the range is a read of its own rather than
    /// a whole record read and thrown away.
    fn get_range(
        &self,
        cf: &str,
        key: &[u8],
        offset: u64,
        len: usize,
    ) -> StoreResult<Option<Value>> {
        let column = self.classify(cf)?;
        let key = self.record_key(column, key)?;
        self.get_range(&key, offset, len).map_err(StoreError::from)
    }

    /// The same window awaited rather than waited for, never through a mapping
    async fn get_range_wait(
        &self,
        cf: &str,
        key: &[u8],
        offset: u64,
        len: usize,
    ) -> StoreResult<Option<Value>> {
        let column = self.classify(cf)?;
        let key = self.record_key(column, key)?;
        self.get_range_wait(&key, offset, len)
            .await
            .map_err(StoreError::from)
    }

    /// The same read awaited rather than waited for, never through a mapping
    async fn get_wait(&self, cf: &str, key: &[u8]) -> StoreResult<Option<Value>> {
        let column = self.classify(cf)?;
        let key = self.record_key(column, key)?;
        self.get_wait(&key).await.map_err(StoreError::from)
    }

    /// Ask the device for every key at once, awaited, answered in the order asked
    async fn get_many_wait(&self, cf: &str, keys: &[&[u8]]) -> StoreResult<Vec<Option<Value>>> {
        let column = self.classify(cf)?;
        let mut held = HeldKeys::take();
        let record_keys = &mut held.0;
        for key in keys {
            record_keys.push(self.record_key(column, key)?);
        }
        self.get_many_wait(record_keys)
            .await
            .map_err(StoreError::from)
    }

    fn put(&self, cf: &str, key: &[u8], value: &[u8]) -> StoreResult<()> {
        let column = self.classify(cf)?;
        let key = self.record_key(column, key)?;
        self.put(&key, value).map_err(StoreError::from)
    }

    /// The same write awaited, with the sync forwarded rather than taken here
    async fn put_wait(&self, cf: &str, key: &[u8], value: &[u8]) -> StoreResult<()> {
        let column = self.classify(cf)?;
        let key = self.record_key(column, key)?;
        self.put_owned_wait(&key, value.to_vec())
            .await
            .map_err(StoreError::from)
    }

    fn delete(&self, cf: &str, key: &[u8]) -> StoreResult<()> {
        let column = self.classify(cf)?;
        let key = self.record_key(column, key)?;
        self.delete(&key).map_err(StoreError::from)
    }

    fn contains(&self, cf: &str, key: &[u8]) -> StoreResult<bool> {
        let column = self.classify(cf)?;
        let key = self.record_key(column, key)?;
        Ok(self.contains(&key)?)
    }

    /// Apply a batch as one durability point rather than one per record
    fn write_batch(&self, batch: WriteBatch) -> StoreResult<()> {
        let writes = self.record_writes(batch)?;
        self.apply_batch(writes).map_err(StoreError::from)
    }

    /// The same batch awaited, still one durability point for the whole of it
    async fn write_batch_wait(&self, batch: WriteBatch) -> StoreResult<()> {
        let writes = self.record_writes(batch)?;
        self.apply_batch_wait(writes)
            .await
            .map_err(StoreError::from)
    }

    /// Drop a half-open key range with one tombstone covering the whole of it
    fn delete_range(&self, cf: &str, start: &[u8], end: &[u8]) -> StoreResult<()> {
        let column = self.classify(cf)?;
        let start = self.bound_key(column, start)?;
        let end = self.bound_bytes(column, end);
        self.delete_range(&start, Some(&end))
            .map_err(StoreError::from)
    }

    fn iter(&self, cf: &str) -> StoreResult<StoreIter<'_>> {
        let column = self.classify(cf)?;
        Ok(self.scan_values(Scope::empty(Direction::Asc), column))
    }

    fn iter_prefix(&self, cf: &str, prefix: &[u8]) -> StoreResult<StoreIter<'_>> {
        let column = self.classify(cf)?;
        let scope = Scope {
            prefix: Some(prefix.to_vec()),
            ..Scope::empty(Direction::Asc)
        };
        Ok(self.scan_values(scope, column))
    }

    fn iter_keys_prefix(&self, cf: &str, prefix: &[u8]) -> StoreResult<Vec<Vec<u8>>> {
        let column = self.classify(cf)?;
        let mut scope = Scope {
            prefix: Some(prefix.to_vec()),
            ..Scope::empty(Direction::Asc)
        };
        Ok(self.scan_keys(&mut scope, column))
    }

    /// Exact key count under a prefix, from the counters wherever they answer it
    ///
    /// An empty prefix is the column's live count and a prefix naming one shard is a
    /// count that shard already keeps. Only a prefix cutting across shards steps
    /// keys, and never payloads
    /// 
    /// One page of a column, in no promised order, resumable by an opaque mark.
    fn sweep(&self, cf: &str, from: Option<&[u8]>, limit: usize) -> StoreResult<SweptPage> {
        let column = self.classify(cf)?;
        let mut page = KeyPage::default();
        let next = self.sweep_column(column, from, limit, &mut page);

        let mut keys: Vec<Vec<u8>> = Vec::with_capacity(page.len());
        for at in 0..page.len() {
            keys.push(page.key_at(at));
        }
        let borrowed: Vec<&[u8]> = keys.iter().map(|key| key.as_slice()).collect();
        let values = Store::get_many(self, cf, &borrowed)?;

        let mut rows = Vec::with_capacity(keys.len());
        for (key, value) in keys.into_iter().zip(values) {
            // A key the page named and the read no longer finds was retired
            // between the two, which a sweep simply does not hand out.
            if let Some(value) = value {
                rows.push((key, value));
            }
        }
        Ok((rows, next))
    }

    /// One page under a shard-aligned prefix, in no promised order
    fn sweep_prefix(
        &self,
        cf: &str,
        prefix: &[u8],
        from: Option<&[u8]>,
        limit: usize,
    ) -> StoreResult<SweptPage> {
        let column = self.classify(cf)?;
        let mut page = KeyPage::default();
        let next = self.sweep_column_prefix(column, prefix, from, limit, &mut page);

        let mut keys: Vec<Vec<u8>> = Vec::with_capacity(page.len());
        for at in 0..page.len() {
            keys.push(page.key_at(at));
        }
        let borrowed: Vec<&[u8]> = keys.iter().map(|key| key.as_slice()).collect();
        let values = Store::get_many(self, cf, &borrowed)?;

        let mut rows = Vec::with_capacity(keys.len());
        for (key, value) in keys.into_iter().zip(values) {
            if let Some(value) = value {
                rows.push((key, value));
            }
        }
        Ok((rows, next))
    }

    fn count_prefix(&self, cf: &str, prefix: &[u8]) -> StoreResult<u64> {
        let column = self.classify(cf)?;
        let counted = self.counters_agree(column);
        if prefix.is_empty() && counted {
            if let Some(totals) = self.column_totals(column) {
                return Ok(totals.count);
            }
        }
        if counted {
            if let Some(totals) = self.prefix_totals(column, prefix) {
                return Ok(totals.count);
            }
        }

        let mut scope = Scope {
            prefix: Some(prefix.to_vec()),
            ..Scope::empty(Direction::Asc)
        };
        let mut page = Page::keys_only(&scope, column, self.serves(column));
        let mut total = 0u64;
        // A count reads each key and keeps none, so one buffer serves the whole walk.
        let mut key = Vec::new();
        while page.next_into(self, &mut key).is_some() {
            match scope.locate(&key) {
                Position::Past => break,
                Position::Before => continue,
                Position::Inside => total += 1,
            }
        }
        Ok(total)
    }

    fn bytes_prefix(&self, cf: &str, prefix: &[u8]) -> StoreResult<Option<u64>> {
        let column = self.classify(cf)?;
        let counted = self.counters_agree(column);
        if prefix.is_empty() && counted {
            if let Some(totals) = self.column_totals(column) {
                return Ok(Some(totals.bytes.to_bytes()));
            }
        }
        if counted {
            if let Some(totals) = self.prefix_totals(column, prefix) {
                return Ok(Some(totals.bytes.to_bytes()));
            }
        }

        // The walk reads each key and where its record sits, and no payload at all.
        let mut scope = Scope {
            prefix: Some(prefix.to_vec()),
            ..Scope::empty(Direction::Asc)
        };
        let mut page = Page::entries_only(&scope, column, self.serves(column));
        let mut total = 0u64;
        let mut key = Vec::new();
        while let Some((found, _)) = page.next_into(self, &mut key) {
            match scope.locate(&key) {
                Position::Past => break,
                Position::Before => continue,
                Position::Inside => {
                    total += found.map_or(0, |entry| u64::from(entry.loc.len));
                }
            }
        }
        Ok(Some(total))
    }

    fn walk_from(
        &self,
        cf: &str,
        start: &[u8],
        direction: Direction,
        hint: usize,
        take: &mut dyn FnMut(&[u8], &[u8]) -> bool,
    ) -> StoreResult<()> {
        let mut walk = self.iter_lent(cf, Some(start), direction, hint)?;
        while let Some((key, value)) = walk.next() {
            if !take(key, value) {
                break;
            }
        }
        Ok(())
    }

    fn iter_from(
        &self,
        cf: &str,
        start: &[u8],
        direction: Direction,
    ) -> StoreResult<StoreIter<'_>> {
        let column = self.classify(cf)?;
        let scope = match direction {
            Direction::Asc => Scope {
                lower: Some(start.to_vec()),
                ..Scope::empty(Direction::Asc)
            },
            Direction::Desc => Scope {
                upper_inclusive: Some(start.to_vec()),
                ..Scope::empty(Direction::Desc)
            },
        };
        Ok(self.scan_values(scope, column))
    }

    fn iter_range(&self, cf: &str, start: &[u8], end: &[u8]) -> StoreResult<StoreIter<'_>> {
        let column = self.classify(cf)?;
        let scope = Scope {
            lower: Some(start.to_vec()),
            upper: Some(end.to_vec()),
            ..Scope::empty(Direction::Asc)
        };
        Ok(self.scan_values(scope, column))
    }

    fn actual_size_bytes(&self) -> StoreResult<u64> {
        directory_size_bytes(self.root())
    }

    fn available_disk_bytes(&self) -> StoreResult<Option<u64>> {
        Ok(available_bytes(self.root()))
    }

    fn live_data_size_bytes(&self) -> StoreResult<Option<u64>> {
        Ok(Some(self.totals().bytes.to_bytes()))
    }

    fn key_count_estimate(&self, cf: &str) -> StoreResult<Option<u64>> {
        match self.classify(cf) {
            Ok(column) => Ok(self.column_totals(column).map(|totals| totals.count)),
            Err(_) => Ok(None),
        }
    }

    fn cf_disk_usage(&self) -> StoreResult<Vec<CfDiskUsage>> {
        let mut usage = Vec::with_capacity(self.columns().len());
        for spec in self.columns() {
            let totals = self.column_totals(spec.id);
            usage.push(CfDiskUsage {
                cf: spec.name.to_string(),
                volume: StoreVolume::Bulk,
                sst_bytes: 0,
                blob_bytes: totals.map_or(0, |totals| totals.bytes.to_bytes()),
                num_keys: totals.map_or(0, |totals| totals.count),
            });
        }
        Ok(usage)
    }

    fn reclaim_space(&self) -> StoreResult<()> {
        self.compact_once().map(|_| ()).map_err(StoreError::from)
    }

    fn maintain(&self) -> StoreResult<()> {
        self.maintain_once().map_err(StoreError::from)
    }

    fn disk_volumes(&self) -> StoreResult<Vec<DiskVolume>> {
        Ok(vec![DiskVolume {
            volume: StoreVolume::Bulk,
            used_bytes: directory_size_bytes(self.root())?,
            free_bytes: available_bytes(self.root()),
        }])
    }
}

/// The bounds and direction of one ordered playback over a column's key space
struct Scope {
    lower: Option<Vec<u8>>,
    upper: Option<Vec<u8>>,
    upper_inclusive: Option<Vec<u8>>,
    prefix: Option<Vec<u8>>,
    direction: Direction,
}

impl Scope {
    /// An unbounded playback in the given direction
    fn empty(direction: Direction) -> Scope {
        Scope {
            lower: None,
            upper: None,
            upper_inclusive: None,
            prefix: None,
            direction,
        }
    }

    /// Where one key sits relative to the playback, in a single pass over the bounds
    ///
    /// Keys arrive in playback order, so the first key past the far bound ends the
    /// playback and the near bound retires once one key has satisfied it.
    fn locate(&mut self, key: &[u8]) -> Position {
        if let Some(prefix) = &self.prefix {
            if !key.starts_with(prefix) {
                return self.side(key > prefix.as_slice());
            }
        }
        if let Some(lower) = &self.lower {
            if key < lower.as_slice() {
                return self.side(false);
            }
            if self.direction == Direction::Asc {
                self.lower = None;
            }
        }
        if let Some(upper) = &self.upper {
            if key >= upper.as_slice() {
                return self.side(true);
            }
        }
        if let Some(upper) = &self.upper_inclusive {
            if key > upper.as_slice() {
                return self.side(true);
            }
            if self.direction == Direction::Desc {
                self.upper_inclusive = None;
            }
        }
        Position::Inside
    }

    /// Whether a key above or below the playback is still ahead of it or behind it
    fn side(&self, is_above: bool) -> Position {
        match (self.direction, is_above) {
            (Direction::Asc, true) | (Direction::Desc, false) => Position::Past,
            (Direction::Asc, false) | (Direction::Desc, true) => Position::Before,
        }
    }

    /// The key the playback starts from, derived from its near bound
    fn start_bound(&self) -> Bound<Vec<u8>> {
        let near = match self.direction {
            Direction::Asc => self.lower.as_ref().or(self.prefix.as_ref()),
            Direction::Desc => self.upper_inclusive.as_ref(),
        };
        match near {
            Some(bound) => Bound::Included(bound.clone()),
            None => Bound::Unbounded,
        }
    }
}

impl ReelStore {
    /// Resolve a column family name to the column the reel serves it from
    fn classify(&self, cf: &str) -> StoreResult<ColumnId> {
        match self.column_spec(cf) {
            Some(spec) => Ok(spec.id),
            None => Err(StoreError::ColumnFamilyNotFound(cf.to_string())),
        }
    }

    /// Resolve a batch to the writes the reel takes, refusing an unserved family
    ///
    /// Every family is classified before anything is converted, so a batch naming
    /// one the reel does not serve is refused whole rather than half applied.
    fn record_writes(&self, batch: WriteBatch) -> StoreResult<Vec<RecordWrite>> {
        for op in batch.iter() {
            self.classify(op.cf())?;
        }
        let mut writes = Vec::with_capacity(batch.len());
        for op in batch {
            match op {
                BatchOp::Put { cf, key, value } => {
                    let column = self.classify(&cf)?;
                    writes.push(RecordWrite::Put {
                        key: self.record_key(column, &key)?,
                        payload: value,
                    });
                }
                BatchOp::Delete { cf, key } => {
                    let column = self.classify(&cf)?;
                    writes.push(RecordWrite::Delete {
                        key: self.record_key(column, &key)?,
                    });
                }
            }
        }
        Ok(writes)
    }

    /// How one column's keys are shaped, for a family the reel holds
    fn key_shape(&self, column: ColumnId) -> Option<KeyWidth> {
        self.index().spec(column).map(|spec| spec.key_width)
    }

    /// Whether the reel holds this column at all
    fn serves(&self, column: ColumnId) -> bool {
        self.key_shape(column).is_some()
    }

    /// A key addressed to a column, rejecting one the column cannot hold
    fn record_key(&self, column: ColumnId, key: &[u8]) -> StoreResult<RecordKey> {
        let shape = self
            .key_shape(column)
            .ok_or_else(|| StoreError::Database(format!("no column {}", column.as_u8())))?;
        if !shape.admits(key.len()) {
            return Err(StoreError::Database(match shape {
                KeyWidth::Fixed(width) => format!(
                    "key of {} bytes does not fit a column keyed at {width}",
                    key.len(),
                ),
                KeyWidth::Variable => format!(
                    "key of {} bytes is wider than the {MAX_KEY_LEN} byte maximum",
                    key.len(),
                ),
            }));
        }
        RecordKey::from_bytes(column, key).map_err(|error| StoreError::Database(error.to_string()))
    }

    /// A range bound as a key, at whatever shape the column's keys take
    fn bound_key(&self, column: ColumnId, bound: &[u8]) -> StoreResult<RecordKey> {
        let bytes = self.bound_bytes(column, bound);
        RecordKey::from_bytes(column, &bytes)
            .map_err(|error| StoreError::Database(error.to_string()))
    }

    /// A range bound at the shape the column's keys take
    ///
    /// A fixed column extends a short bound with zeros, since a bound shorter than a
    /// key names the low end of the range that prefix covers, which is the right
    /// reading for both ends of a half-open range. A variable column has no width to
    /// extend to.
    fn bound_bytes(&self, column: ColumnId, bound: &[u8]) -> Vec<u8> {
        let Some(KeyWidth::Fixed(width)) = self.key_shape(column) else {
            return bound.to_vec();
        };
        let width = usize::from(width);
        let mut bytes = vec![0u8; width];
        let take = bound.len().min(width);
        bytes[..take].copy_from_slice(&bound[..take]);
        bytes
    }

    /// Walk keys alone from a bound, values never read
    ///
    /// The cursor a one-key question wants: a highest-key ask through a value
    /// playback stages payloads it drops, where this stages nothing.
    pub fn iter_keys_from(
        &self,
        cf: &str,
        start: Option<&[u8]>,
        direction: Direction,
    ) -> StoreResult<KeyIter<'_>> {
        let column = self.classify(cf)?;
        let scope = match (start, direction) {
            (None, direction) => Scope::empty(direction),
            (Some(start), Direction::Asc) => Scope {
                lower: Some(start.to_vec()),
                ..Scope::empty(Direction::Asc)
            },
            (Some(start), Direction::Desc) => Scope {
                upper_inclusive: Some(start.to_vec()),
                ..Scope::empty(Direction::Desc)
            },
        };
        Ok(KeyIter {
            store: self,
            page: Page::keys_only(&scope, column, self.serves(column)),
            scope,
            spare: Vec::new(),
        })
    }

    fn scan_keys(&self, scope: &mut Scope, column: ColumnId) -> Vec<Vec<u8>> {
        let mut keys = Vec::new();
        let mut page = Page::keys_only(scope, column, self.serves(column));
        // One buffer for the whole walk, so a skipped key costs no allocation.
        let mut key = Vec::new();
        while page.next_into(self, &mut key).is_some() {
            match scope.locate(&key) {
                Position::Past => break,
                Position::Before => continue,
                Position::Inside => keys.push(std::mem::take(&mut key)),
            }
        }
        keys
    }

    /// The entries inside a playback, in key order, reading each payload only when
    /// the caller pulls it
    fn scan_values(&self, scope: Scope, column: ColumnId) -> StoreIter<'_> {
        Box::new(self.playback(scope, column))
    }

    fn playback(&self, scope: Scope, column: ColumnId) -> Playback<'_> {
        let page = Page::with_lens(&scope, column, self.serves(column));
        Playback {
            store: self,
            scope,
            column,
            page,
            ready: VecDeque::new(),
            spare: Vec::new(),
            staged: Vec::new(),
            placed: Vec::new(),
            held: Vec::new(),
            run: PLAYBACK_RUN_MIN,
            is_done: false,
        }
    }

    /// The same playback with its first page and run sized to what a caller wants
    ///
    /// A playback opens at the minimum page and run and doubles as it goes, which is
    /// wrong for a caller that wants one row. A hint starts both at the caller's own
    /// count, and zero keeps the defaults.
    fn playback_sized(&self, scope: Scope, column: ColumnId, hint: usize) -> Playback<'_> {
        let mut playback = self.playback(scope, column);
        if hint != 0 {
            playback.run = hint.clamp(1, PLAYBACK_RUN_MAX);
            playback.page.size = hint.clamp(1, PLAYBACK_PAGE_MAX);
        }
        playback
    }

    /// A walk that lends both halves of each entry instead of handing them over
    ///
    /// `StoreIter` fixes its item at `(Vec<u8>, Value)`, so every caller takes an
    /// owned key whether it wanted one or not. Here the key is lent from a buffer
    /// the walk takes back on the next step.
    pub fn iter_lent(
        &self,
        cf: &str,
        start: Option<&[u8]>,
        direction: Direction,
        hint: usize,
    ) -> StoreResult<LentIter<'_>> {
        let column = self.classify(cf)?;
        let scope = match (start, direction) {
            (None, direction) => Scope::empty(direction),
            (Some(start), Direction::Asc) => Scope {
                lower: Some(start.to_vec()),
                ..Scope::empty(Direction::Asc)
            },
            (Some(start), Direction::Desc) => Scope {
                upper_inclusive: Some(start.to_vec()),
                ..Scope::empty(Direction::Desc)
            },
        };
        Ok(LentIter {
            playback: self.playback_sized(scope, column, hint),
        })
    }
}

/// Keys one column contributes to a playback, pulled a page at a time
///
/// A page is a contiguous run of the index in playback order, so the next page picks
/// up strictly past the last key the previous one carried. The index lock is taken
/// once per page and never held across a payload read.
struct Page {
    /// Keys the last page pulled, with whatever the column carries beside them
    buffered: KeyPage,

    /// Whether the reel holds the column at all, since one it does not has no keys
    serves: bool,

    /// How many of the buffered keys the caller has already stepped past
    taken: usize,

    /// Where the playback has reached, holding a paged column's footers open
    playback: Option<PlaybackCursor>,

    /// Keys the next page asks the index for, doubling to the ceiling
    size: usize,
}

/// Where a key's record sits, beside the value the page already carries for it
type Placed = (Option<Entry>, Option<Arc<[u8]>>);

impl Page {
    /// A cursor over keys alone, for a playback that reads no payloads
    fn keys_only(scope: &Scope, column: ColumnId, serves: bool) -> Page {
        Page::open(scope, column, serves, KeyPage::default())
    }

    /// A cursor carrying each key's payload length, for a playback that stages reads
    fn with_lens(scope: &Scope, column: ColumnId, serves: bool) -> Page {
        Page::open(scope, column, serves, KeyPage::with_lens())
    }

    /// The same cursor without the values a carrying column keeps, for a walk that
    /// weighs records rather than reading them
    fn entries_only(scope: &Scope, column: ColumnId, serves: bool) -> Page {
        Page::open(scope, column, serves, KeyPage::entries_only())
    }

    fn open(scope: &Scope, column: ColumnId, serves: bool, buffered: KeyPage) -> Page {
        let way = match scope.direction {
            Direction::Asc => Way::Up,
            Direction::Desc => Way::Down,
        };
        let bound = scope.start_bound();
        // A bound wider than any key a column holds matches nothing, so a cursor that
        // will not take it is a playback with no keys in it.
        Page {
            buffered,
            serves,
            taken: 0,
            playback: PlaybackCursor::new(column, way, as_slice_bound(&bound)).ok(),
            size: PLAYBACK_PAGE_MIN,
        }
    }

    /// How many keys the buffer is holding
    fn buffered_count(&self) -> usize {
        self.buffered.len()
    }

    /// The key at a position, written over whatever the buffer was holding
    fn key_into(&self, index: usize, dst: &mut Vec<u8>) {
        dst.clear();
        dst.extend_from_slice(self.buffered.key_ref(index).unwrap_or_default());
    }

    /// The same step with the key written into a buffer the caller keeps
    ///
    /// A walk that steps a million rows hands the same buffer back every time, where
    /// an owned key allocates and frees per row to move as little as eight bytes.
    fn next_into(&mut self, store: &ReelStore, key: &mut Vec<u8>) -> Option<Placed> {
        if !self.serves {
            return None;
        }
        if self.taken < self.buffered_count() {
            let taken = self.taken;
            self.taken += 1;
            self.key_into(taken, key);
            return Some((
                self.buffered.found_at(taken),
                self.buffered.take_carried(taken),
            ));
        }

        let playback = self.playback.as_mut()?;
        if playback.is_done() {
            self.playback = None;
            return None;
        }
        let wanted = self.size;
        // A page a paged column could not read ends the playback short, since an
        // iterator has nowhere to put an error. Counted, because that count is what
        // separates a short playback from a complete one that found less.
        if let Err(error) = store.page_from(playback, wanted, &mut self.buffered) {
            tracing::warn!("a playback stopped at a page it could not read: {error}");
            store.note_unreadable();
            self.buffered.clear();
            self.playback = None;
        }
        self.size = (self.size * 2).min(PLAYBACK_PAGE_MAX);

        if self.buffered_count() == 0 {
            return None;
        }
        self.taken = 1;
        self.key_into(0, key);
        Some((self.buffered.found_at(0), self.buffered.take_carried(0)))
    }
}

/// A lazy ordered playback over one column
///
/// The keys come from the index a page at a time and the payloads from the device a
/// run at a time, so a cold playback is a queue of reads rather than one read, one
/// wait, and the next read.
struct Playback<'store> {
    /// The engine the keys and payloads come from
    store: &'store ReelStore,

    /// Bounds and direction of this playback
    scope: Scope,

    /// The column the playback steps
    column: ColumnId,

    /// Keys pulled from the index, a page at a time
    page: Page,

    /// What the last run read, in key order, waiting for the caller to take it
    ///
    /// Values rather than their bytes, since a merged read hands every record in a
    /// run a window onto one block and owned vectors would copy them apart again.
    ready: VecDeque<(Vec<u8>, Value)>,

    /// Key buffers a lending walk gave back, waiting to be filled again
    ///
    /// Empty for a caller taking owned keys, since those leave and never come back.
    spare: Vec<Vec<u8>>,

    /// The keys one run stages, kept across the runs of a walk
    ///
    /// A walk of a million rows is hundreds of runs, and every list a run works
    /// through is exactly as wide as the run, so they belong to the walk rather than
    /// to the run. What leaves is the key buffers themselves, which come back through
    /// `spare`; the lists holding them stay here.
    staged: Vec<Vec<u8>>,

    /// Where the index placed each of those keys
    placed: Vec<Option<Entry>>,

    /// The payload the index already carries for each of them, where it does
    held: Vec<Option<Arc<[u8]>>>,

    /// Records the next run reads, doubling while the caller keeps draining them
    run: usize,

    /// Whether the playback has run out of keys
    is_done: bool,
}

/// A walk that lends each entry rather than handing it over
///
/// Not an `Iterator`, because the item borrows the walk and the trait cannot say
/// so. Each step invalidates what the last one lent.
pub struct LentIter<'db> {
    playback: Playback<'db>,
}

impl LentIter<'_> {
    /// The next entry, valid until the next step
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Option<(&[u8], &[u8])> {
        let (key, value) = self.playback.next_lent()?;
        Some((key, &**value))
    }
}

/// A walk over keys alone, in index order, reading no payloads
pub struct KeyIter<'db> {
    store: &'db ReelStore,
    page: Page,
    scope: Scope,

    /// One buffer for the whole walk, so a skipped key costs no allocation
    spare: Vec<u8>,
}

impl KeyIter<'_> {
    /// The next key written into a buffer the caller keeps, false at the end
    ///
    /// The lending step, mirroring the one the page itself offers: a caller that
    /// overwrites its own held key on every step has nowhere to give the last one
    /// back to, so the walk writes into the caller's buffer and keeps its own spare.
    pub fn next_into(&mut self, key: &mut Vec<u8>) -> bool {
        loop {
            if self.page.next_into(self.store, key).is_none() {
                return false;
            }
            match self.scope.locate(key) {
                Position::Past => return false,
                Position::Before => continue,
                Position::Inside => return true,
            }
        }
    }
}

impl Iterator for KeyIter<'_> {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Vec<u8>> {
        loop {
            self.page.next_into(self.store, &mut self.spare)?;
            match self.scope.locate(&self.spare) {
                Position::Past => return None,
                Position::Before => continue,
                Position::Inside => return Some(std::mem::take(&mut self.spare)),
            }
        }
    }
}

impl Iterator for Playback<'_> {
    type Item = (Vec<u8>, Value);

    fn next(&mut self) -> Option<(Vec<u8>, Value)> {
        loop {
            if let Some(found) = self.ready.pop_front() {
                return Some(found);
            }
            if self.is_done {
                return None;
            }
            self.read_run();
        }
    }
}

/// A key the playback reached, with what the index held for it and any carried bytes
type ScopedKey = (Vec<u8>, Option<Entry>, Option<Arc<[u8]>>);

impl Playback<'_> {
    /// The next key inside the playback's scope, or nothing once the playback is over
    fn next_in_scope(&mut self) -> Option<ScopedKey> {
        // One buffer for the whole search, so a skipped key costs no allocation.
        let mut key = self.spare.pop().unwrap_or_default();
        loop {
            let (found, carried) = match self.page.next_into(self.store, &mut key) {
                Some(found) => found,
                None => {
                    self.is_done = true;
                    self.recycle(key);
                    return None;
                }
            };
            match self.scope.locate(&key) {
                Position::Past => {
                    self.is_done = true;
                    self.recycle(key);
                    return None;
                }
                Position::Before => continue,
                Position::Inside => {}
            }
            // The width is the one thing a key can fail on, and nothing owned is
            // built here: the run lends these to the read path borrowed.
            if key.len() <= MAX_KEY_LEN {
                return Some((key, found, carried));
            }
        }
    }

    /// Take a key buffer back, up to about a run's worth
    ///
    /// Bounded so a walk that reads far more keys than it hands out cannot turn the
    /// pool into a second copy of the column.
    fn recycle(&mut self, key: Vec<u8>) {
        if self.spare.len() < PLAYBACK_RUN_MAX {
            self.spare.push(key);
        }
    }

    /// The next entry with both halves lent until the caller's next step
    ///
    /// The entry stays in `ready` while the caller reads it and is retired on the
    /// step after, which is what lets the key buffer come back rather than be freed.
    fn next_lent(&mut self) -> Option<(&[u8], &Value)> {
        if let Some((key, _value)) = self.ready.pop_front() {
            self.recycle(key);
        }
        while self.ready.is_empty() {
            if self.is_done {
                return None;
            }
            self.read_run();
        }
        let (key, value) = self.ready.front()?;
        Some((key.as_slice(), value))
    }

    /// Take the next run of keys off the page and read all of their payloads at once
    ///
    /// The run stops at the depth, the byte ceiling, or the end of the playback. The
    /// ceiling is tested after a record is added, so a record larger than the whole
    /// ceiling is read on its own rather than never. The depth doubles per run, so a
    /// caller that stops early pays for what it nearly wanted.
    fn read_run(&mut self) {
        let wanted = self.run;
        // Sized to the run, since these grow to exactly it on a draining walk. Taken
        // out of the walk's own lists so a run past the first buys none of them.
        let mut keys = std::mem::take(&mut self.staged);
        let mut found = std::mem::take(&mut self.placed);
        let mut carried = std::mem::take(&mut self.held);
        keys.clear();
        found.clear();
        carried.clear();
        keys.reserve(wanted);
        found.reserve(wanted);
        carried.reserve(wanted);
        let mut bytes = 0u64;
        self.run = (self.run * 2).min(PLAYBACK_RUN_MAX);

        while keys.len() < wanted && bytes < PLAYBACK_READ_BYTES {
            let Some((key, entry, value)) = self.next_in_scope() else {
                break;
            };
            // A value the page already carries costs the run nothing to stage.
            bytes += match value {
                Some(_) => 0,
                None => entry.map(|entry| u64::from(entry.loc.len)).unwrap_or(0),
            };
            keys.push(key);
            found.push(entry);
            carried.push(value);
        }

        // The entries came off the page the index already built, so the read goes
        // straight to the device rather than resolving these keys a second time.
        let column = self.column;
        let asked: Vec<KeyRef<'_>> = keys.iter().map(|key| KeyRef::new(column, key)).collect();
        let read = self.store.read_found(&asked, &found, &mut carried);
        drop(asked);
        self.placed = found;
        self.held = carried;
        match read {
            Ok(values) => {
                for (key, value) in keys.drain(..).zip(values) {
                    match value {
                        Some(value) => self.ready.push_back((key, value)),
                        None => self.recycle(key),
                    }
                }
                self.staged = keys;
            }
            // A failed run says nothing about which record failed it, so it is read
            // again one at a time and the unreadable record drops out on its own.
            Err(_) => {
                self.read_singly(&mut keys, column);
                self.staged = keys;
            }
        }
    }

    /// Read a run one record at a time, dropping the ones that cannot be read
    ///
    /// The store iterator has no way to carry an error, so a key the playback cannot
    /// read drops out of the results and is counted as unreadable.
    fn read_singly(&mut self, keys: &mut Vec<Vec<u8>>, column: ColumnId) {
        for key in keys.drain(..) {
            // The one place a run builds an owned key, on the path a device error
            // already sent one record at a time.
            let Ok(record) = RecordKey::from_bytes(column, &key) else {
                self.recycle(key);
                continue;
            };
            match self.store.get(&record) {
                Ok(Some(value)) => self.ready.push_back((key, value)),
                Ok(None) => {}
                Err(error) => {
                    tracing::warn!("a playback skipped a record it could not read: {error}");
                    self.store.note_unreadable();
                }
            }
        }
    }
}

fn as_slice_bound(bound: &Bound<Vec<u8>>) -> Bound<&[u8]> {
    match bound {
        Bound::Unbounded => Bound::Unbounded,
        Bound::Included(key) => Bound::Included(key.as_slice()),
        Bound::Excluded(key) => Bound::Excluded(key.as_slice()),
    }
}

/// Bytes free on the filesystem holding a path, or nothing where it cannot be asked
fn available_bytes(root: &Path) -> Option<u64> {
    use std::os::unix::ffi::OsStrExt;

    let path = std::ffi::CString::new(root.as_os_str().as_bytes()).ok()?;
    // SAFETY: statvfs writes the whole struct it is handed, and the path is a
    // NUL-terminated buffer that outlives the call.
    let mut stats = unsafe { std::mem::zeroed::<libc::statvfs>() };
    match unsafe { libc::statvfs(path.as_ptr(), &mut stats) } {
        0 => Some(stats.f_bavail as u64 * stats.f_frsize as u64),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::path::PathBuf;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use crate::units::ByteCount;

    use crate::config::{Preallocate, ReelConfig, SyncPolicy, ThreadBudget};
    use crate::format::column::{Codec, ColumnSet, ColumnSpec, MapShape};
    use crate::io::fault::FaultPlan;
    use crate::io::sim_backend::SimIo;
    use crate::sync::tension::block_on;

    /// Virtual volume root the simulator files live under
    const ROOT: &str = "/bulk";

    const RECORD_CF: &str = "record";
    const BLOB_CF: &str = "blob";
    const ARTIFACT_CF: &str = "artifact";

    const GROUP_PREFIX_LEN: usize = 2;
    const RECORD_KEY_LEN: usize = GROUP_PREFIX_LEN + 32;
    const BLOB_KEY_LEN: usize = 32;
    const ARTIFACT_KEY_LEN: usize = 24;

    /// The columns these cases open the engine with, the way any caller declares its own
    const TEST_COLUMNS: ColumnSet = &[
        ColumnSpec {
            id: ColumnId(1),
            name: RECORD_CF,
            key_width: KeyWidth::Fixed(RECORD_KEY_LEN as u16),
            shard_bytes: GROUP_PREFIX_LEN as u8,
            inline_max: 0,
            row_carry: 0,
            purge_mark: None,
            codec: Codec::None,
            map_shape: MapShape::Tree,
        },
        ColumnSpec {
            id: ColumnId(2),
            name: BLOB_CF,
            key_width: KeyWidth::Fixed(BLOB_KEY_LEN as u16),
            shard_bytes: 1,
            inline_max: 0,
            row_carry: 0,
            purge_mark: None,
            codec: Codec::None,
            map_shape: MapShape::Tree,
        },
        ColumnSpec {
            id: ColumnId(3),
            name: ARTIFACT_CF,
            key_width: KeyWidth::Fixed(ARTIFACT_KEY_LEN as u16),
            shard_bytes: 0,
            inline_max: 0,
            row_carry: 0,
            purge_mark: None,
            codec: Codec::None,
            map_shape: MapShape::Tree,
        },
    ];

    fn config() -> ReelConfig {
        ReelConfig {
            segment_bytes: ByteCount::mb(1),
            alloc_chunk: ByteCount::from_bytes(16_384),
            preallocate: Preallocate::Chunk,
            sync: SyncPolicy::Never,
            active_tails: ThreadBudget::threads(1),
            ..ReelConfig::default()
        }
    }

    fn store() -> ReelStore {
        store_with_io().0
    }

    /// Run work with a thread draining the simulator behind it
    ///
    /// The simulator neither answers at submission nor files from a thread of its
    /// own, so a pending read lands only when somebody drains it.
    fn reaping<Out>(store: &ReelStore, work: impl FnOnce() -> Out) -> Out {
        let is_done = AtomicBool::new(false);
        std::thread::scope(|scope| {
            scope.spawn(|| {
                while !is_done.load(Ordering::Relaxed) {
                    if store.driver().reap().expect("reap") == 0 {
                        std::thread::sleep(Duration::from_micros(50));
                    }
                }
            });
            let out = work();
            is_done.store(true, Ordering::Relaxed);
            out
        })
    }

    fn store_with_io() -> (ReelStore, SimIo) {
        let sim = SimIo::new(FaultPlan::new(1));
        let store = ReelStore::open_with_io(
            PathBuf::from(ROOT),
            config(),
            TEST_COLUMNS,
            Arc::new(sim.clone()),
        )
        .expect("open");
        (store, sim)
    }

    /// The engine seen through the store trait, which is what these cases drive
    ///
    /// The engine's own methods shadow the trait's, so going through the trait has
    /// to be said out loud.
    fn trait_store(store: &ReelStore) -> &dyn Store {
        store
    }

    fn record(group: u16, byte: u8) -> Vec<u8> {
        let mut bytes = group.to_be_bytes().to_vec();
        bytes.extend_from_slice(&[byte; 32]);
        bytes
    }

    fn artifact(high: u64, middle: u64, low: u64) -> Vec<u8> {
        let mut bytes = high.to_be_bytes().to_vec();
        bytes.extend_from_slice(&middle.to_be_bytes());
        bytes.extend_from_slice(&low.to_be_bytes());
        bytes
    }

    fn keys(store: &dyn Store, cf: &str, prefix: &[u8]) -> Vec<Vec<u8>> {
        store.iter_keys_prefix(cf, prefix).expect("keys")
    }

    // a playback asks the device for a run of payloads rather than one at a time
    #[test]
    fn a_walk_reads_in_runs() {
        let (store, sim) = store_with_io();
        let store = trait_store(&store);
        let count = 20usize;
        for byte in 1..=count as u8 {
            store
                .put(RECORD_CF, &record(1, byte), &[byte; 64])
                .expect("put");
        }

        // Records written in one batch are one byte range, so a playback over them
        // takes fewer reads than records.
        let before = sim.read_count();
        let played: Vec<(Vec<u8>, Vec<u8>)> = store
            .iter(RECORD_CF)
            .expect("iter")
            .map(|(key, value)| (key, value.into_vec()))
            .collect();
        let reads = sim.read_count() - before;

        assert_eq!(played.len(), count);
        assert!(
            reads < count as u64,
            "the playback read {reads} times for {count} records"
        );
    }

    // the run starts small and reaches its ceiling only for a playback that keeps going
    #[test]
    fn a_run_ramps_to_its_ceiling() {
        let (store, sim) = store_with_io();
        let store = trait_store(&store);
        let count = PLAYBACK_RUN_MAX * 3;
        for key in 0..count {
            let mut bytes = record(1, 0);
            bytes[2..6].copy_from_slice(&(key as u32).to_be_bytes());
            store.put(RECORD_CF, &bytes, &[7u8; 64]).expect("put");
        }

        // One key wanted, so one run at the floor rather than one at the ceiling.
        let before = sim.read_bytes();
        let first = store.iter(RECORD_CF).expect("iter").next();
        assert!(first.is_some(), "the playback found its first key");
        let taking_one = sim.read_bytes() - before;

        // The whole column wanted, so the run doubles until it reaches the ceiling.
        let before = sim.read_bytes();
        let played = store.iter(RECORD_CF).expect("iter").count();
        let taking_all = sim.read_bytes() - before;
        assert_eq!(played, count);

        // Far more bytes than one run's, so the first caller was never charged for
        // the ceiling.
        assert!(
            taking_all > taking_one * (count / PLAYBACK_RUN_MAX) as u64,
            "taking one key read {taking_one} bytes against {taking_all} for all {count}"
        );
    }

    // a playback answers what a point read of the same keys answers
    #[test]
    fn a_walk_in_runs_answers_the_same() {
        let (store, _sim) = store_with_io();
        let store = trait_store(&store);
        for byte in 1..=40u8 {
            store
                .put(RECORD_CF, &record(1, byte), &[byte; 32])
                .expect("put");
        }
        store.delete(RECORD_CF, &record(1, 7)).expect("delete");

        let played: Vec<(Vec<u8>, Vec<u8>)> = store
            .iter(RECORD_CF)
            .expect("iter")
            .map(|(key, value)| (key, value.into_vec()))
            .collect();
        let looked_up: Vec<(Vec<u8>, Vec<u8>)> = played
            .iter()
            .map(|(key, _)| {
                let value = store.get(RECORD_CF, key).expect("get").expect("present");
                (key.clone(), value.into_vec())
            })
            .collect();

        assert_eq!(
            played.len(),
            39,
            "the deleted key is gone and the rest are not"
        );
        assert_eq!(
            played, looked_up,
            "the run read what a point read would have"
        );
    }

    // a batch through the trait answers what the same keys answer one at a time
    #[test]
    fn get_many_matches_the_loop() {
        let store = store();
        let store = trait_store(&store);
        for byte in 1..=6u8 {
            store
                .put(RECORD_CF, &record(1, byte), &[byte; 128])
                .expect("put");
        }

        let asked: Vec<Vec<u8>> = (1..=8u8).map(|byte| record(1, byte)).collect();
        let borrowed: Vec<&[u8]> = asked.iter().map(|key| key.as_slice()).collect();
        let looped: Vec<Option<Vec<u8>>> = borrowed
            .iter()
            .map(|key| store.get(RECORD_CF, key).expect("get"))
            .map(|v| v.map(Value::into_vec))
            .collect();

        let batched: Vec<Option<Vec<u8>>> = store
            .get_many(RECORD_CF, &borrowed)
            .expect("get many")
            .into_iter()
            .map(|found| found.map(Value::into_vec))
            .collect();
        assert_eq!(batched, looped);
        assert_eq!(looped[0], Some(vec![1u8; 128]));
        assert_eq!(looped[7], None);
    }

    // the awaited reads through the trait answer what the blocking ones answer
    //
    // Through the concrete store, since the futures are the backend's own types and
    // the async door is not dispatchable.
    #[test]
    fn awaited_reads_match() {
        let store = store();
        for byte in 1..=6u8 {
            Store::put(&store, RECORD_CF, &record(1, byte), &[byte; 128]).expect("put");
        }

        let asked: Vec<Vec<u8>> = (1..=8u8).map(|byte| record(1, byte)).collect();
        let borrowed: Vec<&[u8]> = asked.iter().map(|key| key.as_slice()).collect();
        let blocked = Store::get_many(&store, RECORD_CF, &borrowed).expect("get many");

        let (awaited, one) = reaping(&store, || {
            (
                block_on(Store::get_many_wait(&store, RECORD_CF, &borrowed)).expect("awaited many"),
                block_on(Store::get_wait(&store, RECORD_CF, &asked[0])).expect("awaited get"),
            )
        });

        assert_eq!(awaited, blocked);
        assert_eq!(one, blocked[0]);
        assert!(blocked[0].is_some() && blocked[7].is_none());
    }

    // an unserved family is refused by the awaited reads as well
    #[test]
    fn awaited_reads_refuse_an_unserved_family() {
        let store = store();

        let asked: Vec<&[u8]> = vec![b"anything"];
        assert!(block_on(Store::get_wait(&store, "not_a_reel_family", asked[0])).is_err());
        assert!(block_on(Store::get_many_wait(&store, "not_a_reel_family", &asked)).is_err());
    }

    // the awaited writes through the trait land what the blocking ones land
    #[test]
    fn awaited_writes_match() {
        let store = store();
        Store::put(&store, RECORD_CF, &record(1, 1), &[0x11; 128]).expect("put");

        block_on(Store::put_wait(
            &store,
            RECORD_CF,
            &record(1, 2),
            &[0x22; 128],
        ))
        .expect("awaited put");
        let mut batch = WriteBatch::new();
        batch.put(RECORD_CF, &record(1, 3), &[0x33; 128]);
        batch.delete(RECORD_CF, &record(1, 1));
        block_on(Store::write_batch_wait(&store, batch)).expect("awaited batch");

        assert_eq!(
            Store::get(&store, RECORD_CF, &record(1, 1)).expect("get"),
            None
        );
        assert_eq!(
            Store::get(&store, RECORD_CF, &record(1, 2))
                .expect("get")
                .map(Value::into_vec),
            Some(vec![0x22; 128]),
        );
        assert_eq!(
            Store::get(&store, RECORD_CF, &record(1, 3))
                .expect("get")
                .map(Value::into_vec),
            Some(vec![0x33; 128]),
        );
    }

    // an unserved family is refused by the awaited writes as well
    #[test]
    fn awaited_writes_refuse_an_unserved_family() {
        let store = store();

        assert!(block_on(Store::put_wait(
            &store,
            "not_a_reel_family",
            b"key",
            b"value"
        ))
        .is_err());

        let mut batch = WriteBatch::new();
        batch.put("not_a_reel_family", b"key", b"value");
        assert!(block_on(Store::write_batch_wait(&store, batch)).is_err());
    }

    // an unserved family is refused by the batch just as it is by the single read
    #[test]
    fn get_many_refuses_an_unserved_family() {
        let store = store();
        let store = trait_store(&store);

        let asked: Vec<&[u8]> = vec![b"anything"];
        assert!(store.get_many("not_a_reel_family", &asked).is_err());
    }

    // the reel serves the columns it was opened with and refuses every other name
    #[test]
    fn serves_the_declared_columns() {
        let store = store();
        let store = trait_store(&store);

        for spec in TEST_COLUMNS {
            let count = store.count_prefix(spec.name, &[]).expect("count");
            assert_eq!(count, 0, "{} is not served", spec.name);
        }
        assert!(matches!(
            store.count_prefix("meta", &[]),
            Err(StoreError::ColumnFamilyNotFound(_))
        ));
    }

    // every declared column round trips through the store trait at its own width
    #[test]
    fn every_column_roundtrips() {
        let store = store();
        let store = trait_store(&store);

        store
            .put(RECORD_CF, &record(7, 1), &[0x11; 400])
            .expect("record");
        store.put(BLOB_CF, &[0x22; 32], &[0x22; 900]).expect("blob");
        store
            .put(ARTIFACT_CF, &artifact(3, 4, 5), &[0x33; 128])
            .expect("artifact");

        assert_eq!(
            store.get(RECORD_CF, &record(7, 1)).expect("get"),
            Some(Value::new(vec![0x11; 400]))
        );
        assert_eq!(
            store.get(BLOB_CF, &[0x22; 32]).expect("get"),
            Some(Value::new(vec![0x22; 900]))
        );
        assert_eq!(
            store.get(ARTIFACT_CF, &artifact(3, 4, 5)).expect("get"),
            Some(Value::new(vec![0x33; 128]))
        );
    }

    // a key that is not the column's width is refused rather than padded
    #[test]
    fn wrong_width_key_refused() {
        let store = store();
        let store = trait_store(&store);

        assert!(store.put(BLOB_CF, &[0u8; 34], &[0x11; 8]).is_err());
        assert!(store.get(ARTIFACT_CF, &[0u8; 32]).is_err());
    }

    // a playback hands back one column's keys in key order and stops at its bounds
    #[test]
    fn walks_in_key_order() {
        let store = store();
        let store = trait_store(&store);
        for (group, byte) in [(9u16, 2u8), (1, 1), (9, 1), (300, 1)] {
            store
                .put(RECORD_CF, &record(group, byte), &[byte; 64])
                .expect("put");
        }
        store.put(BLOB_CF, &[0x44; 32], &[0x44; 64]).expect("blob");

        let all: Vec<Vec<u8>> = store
            .iter(RECORD_CF)
            .expect("iter")
            .map(|(key, _)| key)
            .collect();
        assert_eq!(
            all,
            vec![record(1, 1), record(9, 1), record(9, 2), record(300, 1)]
        );

        let ranged: Vec<Vec<u8>> = store
            .iter_range(RECORD_CF, &record(9, 1), &record(300, 1))
            .expect("range")
            .map(|(key, _)| key)
            .collect();
        assert_eq!(ranged, vec![record(9, 1), record(9, 2)]);

        let descending: Vec<Vec<u8>> = store
            .iter_from(RECORD_CF, &record(9, 2), Direction::Desc)
            .expect("desc")
            .map(|(key, _)| key)
            .collect();
        assert_eq!(descending, vec![record(9, 2), record(9, 1), record(1, 1)]);
    }

    // a prefix playback stays inside its prefix and never crosses columns
    #[test]
    fn prefix_walk_stays_inside() {
        let store = store();
        let store = trait_store(&store);
        for group in [6u16, 7, 8] {
            store
                .put(RECORD_CF, &record(group, 1), &[0x11; 64])
                .expect("put");
        }

        let found = keys(store, RECORD_CF, &7u16.to_be_bytes());

        assert_eq!(found, vec![record(7, 1)]);
        assert_eq!(
            store
                .count_prefix(RECORD_CF, &7u16.to_be_bytes())
                .expect("count"),
            1
        );
        assert_eq!(store.count_prefix(RECORD_CF, &[]).expect("count"), 3);
    }

    // stored bytes under a prefix, from the counters and from the walk alike
    #[test]
    fn bytes_under_a_prefix() {
        let store = store();
        let store = trait_store(&store);
        for group in [6u16, 7, 8] {
            store
                .put(RECORD_CF, &record(group, 1), &[0x11; 64])
                .expect("put");
        }
        store
            .put(RECORD_CF, &record(7, 2), &[0x22; 16])
            .expect("put");

        // The shard prefix is answered from the counters, the deeper one by a walk,
        // and the two have to agree about the same records.
        assert_eq!(
            store.bytes_prefix(RECORD_CF, &7u16.to_be_bytes()).expect("bytes"),
            Some(80)
        );
        assert_eq!(store.bytes_prefix(RECORD_CF, &[]).expect("bytes"), Some(208));

        let mut prefix = 7u16.to_be_bytes().to_vec();
        prefix.push(2);
        assert_eq!(store.bytes_prefix(RECORD_CF, &prefix).expect("bytes"), Some(16));
    }

    // an overwritten record is weighed once, at the length it now holds
    #[test]
    fn bytes_follow_the_live_version() {
        let store = store();
        let store = trait_store(&store);
        store.put(RECORD_CF, &record(7, 1), &[0x11; 64]).expect("put");
        store.put(RECORD_CF, &record(7, 1), &[0x11; 8]).expect("overwrite");

        assert_eq!(store.bytes_prefix(RECORD_CF, &[]).expect("bytes"), Some(8));

        store.delete(RECORD_CF, &record(7, 1)).expect("delete");
        assert_eq!(store.bytes_prefix(RECORD_CF, &[]).expect("bytes"), Some(0));
    }

    // a count that cuts across shards steps the keys rather than the counters
    #[test]
    fn count_across_shards() {
        let store = store();
        let store = trait_store(&store);
        for byte in [1u8, 2, 3] {
            store
                .put(RECORD_CF, &record(7, byte), &[byte; 64])
                .expect("put");
        }

        let mut prefix = 7u16.to_be_bytes().to_vec();
        prefix.push(2);

        assert_eq!(store.count_prefix(RECORD_CF, &prefix).expect("count"), 1);
    }

    // a batch spanning two columns lands both of them
    #[test]
    fn batch_spans_columns() {
        let store = store();
        let store = trait_store(&store);
        let mut batch = WriteBatch::new();
        batch.put(RECORD_CF, &record(7, 1), &[0x11; 64]);
        batch.put(BLOB_CF, &[0x22; 32], &[0x22; 64]);
        batch.delete(RECORD_CF, &record(7, 9));

        store.write_batch(batch).expect("batch");

        assert!(store.contains(RECORD_CF, &record(7, 1)).expect("contains"));
        assert!(store.contains(BLOB_CF, &[0x22; 32]).expect("contains"));
    }

    // a range delete through the trait drops the keys the range covers
    #[test]
    fn range_delete_through_the_trait() {
        let store = store();
        let store = trait_store(&store);
        for group in [6u16, 7, 8] {
            store
                .put(RECORD_CF, &record(group, 1), &[0x11; 64])
                .expect("put");
        }

        store
            .delete_range(RECORD_CF, &7u16.to_be_bytes(), &8u16.to_be_bytes())
            .expect("range delete");

        assert_eq!(
            keys(store, RECORD_CF, &[]),
            vec![record(6, 1), record(8, 1)]
        );
    }

    // the usage report names every column the reel serves
    #[test]
    fn usage_covers_every_family() {
        let store = store();
        let store = trait_store(&store);
        store
            .put(RECORD_CF, &record(7, 1), &[0x11; 400])
            .expect("put");

        let usage = store.cf_disk_usage().expect("usage");
        let named: Vec<String> = usage.iter().map(|entry| entry.cf.clone()).collect();

        for spec in TEST_COLUMNS {
            assert!(
                named.contains(&spec.name.to_string()),
                "{} is missing from the usage report",
                spec.name
            );
        }
    }
}
