//! Puts, deletes and batch writes, planned the same through either door

use std::sync::atomic::Ordering;

use crate::append::{BatchRecord, BatchWrite, Commit, Committed};
use crate::error::{ReelError, Result};
use crate::format::column::RecordKey;

use super::{read_only, BatchKey, KeyOp, Planned, RecordWrite, ReelStore};
use crate::index::column::KeyMove;
use crate::index::map::RangeMove;

impl ReelStore {
    /// Append or overwrite one payload, landing its location in the index
    pub fn put(&self, key: &RecordKey, payload: &[u8]) -> Result<()> {
        self.put_owned(key, payload.to_vec())
    }

    /// Owned-payload put that hands the buffer to the tail without a copy
    pub fn put_owned(&self, key: &RecordKey, payload: Vec<u8>) -> Result<()> {
        let planned = self.plan_put(key, payload)?;
        // The tail owns the key it queues, and the index insert below needs it too.
        let committed = self.reel.put(
            key.clone(),
            planned.payload,
            planned.codec,
            Commit::PerRecord,
        )?;
        // A slow put stands here with its record down and the index not yet moved
        crate::sync::rendezvous::at("put/landed");
        self.index.insert(key, committed.loc, committed.lsn)?;
        Ok(())
    }

    /// The same put awaited, for a caller with a runtime worker to protect
    ///
    /// The record reaches the device on this thread either way; what is awaited is
    /// admission and the sync.
    pub async fn put_owned_wait(&self, key: &RecordKey, payload: Vec<u8>) -> Result<()> {
        let planned = self.plan_put(key, payload)?;
        let committed = self
            .reel
            .put_wait(
                key.clone(),
                planned.payload,
                planned.codec,
                Commit::PerRecord,
            )
            .await?;
        self.index.insert(key, committed.loc, committed.lsn)?;
        Ok(())
    }

    /// Refuse a write the disk cannot take, leaving the reserve for compaction
    ///
    /// Compaction needs somewhere to write the survivors, so a full disk is a dead
    /// end. A volume whose disk will not say how large it is admits everything.
    fn check_capacity(&self, request_bytes: u64) -> Result<()> {
        let used = self.footprint.load(Ordering::Relaxed);
        if self.compactor.can_admit_foreground(used, request_bytes) {
            return Ok(());
        }
        Err(ReelError::Rejected(format!(
            "the volume holds {used} bytes and {request_bytes} more would pass what \
             is left for compaction to work in",
        )))
    }

    fn plan_put(&self, key: &RecordKey, payload: Vec<u8>) -> Result<Planned> {
        if self.is_read_only {
            return Err(read_only());
        }
        self.check_column(key)?;
        self.check_capacity(payload.len() as u64)?;
        let (payload, codec) =
            crate::append::codec::admit(self.index.codec_of(key.column), payload);
        Ok(Planned { payload, codec })
    }

    /// Apply a batch of writes as one reservation, one write, and one sync
    ///
    /// A batch is one durability point: the sync is taken once the last record has
    /// landed, and the index moves only after it comes back clean.
    pub fn apply_batch(&self, writes: Vec<RecordWrite>) -> Result<()> {
        let Some((records, keys)) = self.plan_batch(writes)? else {
            return Ok(());
        };

        let committed = self.reel.write_batch(records)?;
        self.reel.sync_if_owed()?;
        self.publish_batch(&keys, &committed)
    }

    /// The same batch awaited, one submission and one durability point
    ///
    /// The plan and the publish are the blocking batch's own, so the only thing a
    /// row racing the doors sees is where the two waits went.
    pub async fn apply_batch_wait(&self, writes: Vec<RecordWrite>) -> Result<()> {
        let Some((records, keys)) = self.plan_batch(writes)? else {
            return Ok(());
        };

        let committed = self.reel.write_batch_wait(records).await?;
        self.reel.sync_if_owed_wait().await?;
        self.publish_batch(&keys, &committed)
    }

    /// Compress every payload and take what the index will want, before the write
    fn plan_batch(
        &self,
        writes: Vec<RecordWrite>,
    ) -> Result<Option<(Vec<BatchRecord>, Vec<BatchKey>)>> {
        if self.is_read_only {
            return Err(read_only());
        }
        if writes.is_empty() {
            return Ok(None);
        }
        let mut records = Vec::with_capacity(writes.len());
        let mut keys = Vec::with_capacity(writes.len());
        for write in writes {
            let (key, write, op) = match write {
                RecordWrite::Put { key, payload } => {
                    self.check_column(&key)?;
                    let (payload, codec) =
                        crate::append::codec::admit(self.index.codec_of(key.column), payload);
                    (key, BatchWrite::Put(payload, codec), KeyOp::Put)
                }
                RecordWrite::Delete { key } => {
                    self.check_column(&key)?;
                    (key, BatchWrite::Delete, KeyOp::Delete)
                }
                RecordWrite::DeleteRange { start, end } => {
                    self.check_column(&start)?;
                    // The empty range the single-record door refuses, refused here for
                    // the same reason: its row is filed under the start key, which a
                    // sealed search would read as a grave over the key it excluded.
                    if end.as_deref().is_some_and(|end| end <= start.as_slice()) {
                        continue;
                    }
                    let carried = end.clone().unwrap_or_default();
                    (start, BatchWrite::DeleteRange(carried), KeyOp::Range(end))
                }
            };
            records.push(BatchRecord {
                key: key.clone(),
                write,
            });
            keys.push(BatchKey { key, op });
        }
        if records.is_empty() {
            return Ok(None);
        }
        Ok(Some((records, keys)))
    }

    /// Move the index onto a batch that has landed, under the publish barrier
    ///
    /// The barrier lands a read spanning several keys before the batch or after it.
    /// The hold moves the maps, and a key with no map entry at or below the newest
    /// version the spot index was given can wait on a spot shard and read one header.
    /// Settling a displaced sealed version reads more, so it runs after the hold.
    fn publish_batch(&self, keys: &[BatchKey], committed: &[Committed]) -> Result<()> {
        // A slow batch stands here with its records down and the index not yet moved
        crate::sync::rendezvous::at("batch/landed");
        // Built before the barrier, each range held apart with the count of key moves ahead of it
        let mut moves: Vec<KeyMove<'_>> = Vec::with_capacity(keys.len());
        let mut ranges: Vec<RangeMove<'_>> = Vec::new();
        for (planned, landed) in keys.iter().zip(committed) {
            match &planned.op {
                KeyOp::Range(end) => ranges.push(RangeMove {
                    after: moves.len(),
                    start: &planned.key,
                    end: end.as_deref(),
                    lsn: landed.lsn,
                    tombstone: landed.loc,
                }),
                op => moves.push(KeyMove {
                    column: planned.key.column,
                    key: planned.key.as_slice(),
                    loc: landed.loc,
                    lsn: landed.lsn,
                    is_delete: matches!(op, KeyOp::Delete),
                }),
            }
        }

        let (landed, shadowed) = self.index.publish_batch(&moves, &ranges);

        // One answer per key move, so a range gets none and is stepped over
        let mut answers = landed.iter();
        let mut displaced = Vec::new();
        for (planned, record) in keys.iter().zip(committed) {
            if matches!(planned.op, KeyOp::Range(_)) {
                continue;
            }
            let Some(mapped) = answers.next() else {
                break;
            };
            if mapped.may_be_paged() {
                displaced.push((planned.key.clone(), record.lsn));
            }
        }

        // Every displaced key settles past a failure, so one bad read leaves no other counted twice
        let mut settled: Result<()> = Ok(());
        for (key, lsn) in displaced {
            if let Err(error) = self.index.settle_displaced(&key, lsn) {
                settled = settled.and(Err(error));
            }
        }
        shadowed.and(settled)
    }

    /// Append a tombstone and drop the key from the index
    pub fn delete(&self, key: &RecordKey) -> Result<()> {
        if self.is_read_only {
            return Err(read_only());
        }
        self.check_column(key)?;
        let committed = self.reel.delete(key.clone(), Commit::PerRecord)?;
        // A slow delete stands here with its tombstone down and the index not yet moved
        crate::sync::rendezvous::at("delete/landed");
        self.index.remove(key, committed.lsn, committed.loc)?;
        Ok(())
    }

    /// Drop a half-open key range with one tombstone covering the whole of it
    ///
    /// One record stands for however many keys the range holds, and the compactor
    /// carries it until nothing old enough for it to hide is left.
    pub fn delete_range(&self, start: &RecordKey, end: Option<&[u8]>) -> Result<()> {
        if self.is_read_only {
            return Err(read_only());
        }
        self.check_column(start)?;
        // An empty range's record would not be harmless: its row is filed under the
        // start key, and a sealed search reads any tombstone row as a grave, so it
        // would delete the very key the range excluded.
        if end.is_some_and(|end| end <= start.as_slice()) {
            return Ok(());
        }
        let committed = self
            .reel
            .delete_range(start.clone(), end, Commit::PerRecord)?;
        self.index
            .remove_range(start, end, committed.lsn, committed.loc)?;
        Ok(())
    }

    /// Refuse a key naming a column this volume does not serve
    pub(super) fn check_column(&self, key: &RecordKey) -> Result<()> {
        match self.index.spec(key.column) {
            Some(_) => Ok(()),
            None => Err(ReelError::Rejected(format!(
                "column {} is not served by this reel",
                key.column.as_u8()
            ))),
        }
    }
}
