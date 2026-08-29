//! Puts, deletes and batch writes, planned the same through either door

use std::sync::atomic::Ordering;

use crate::append::{BatchRecord, BatchWrite, Commit, Committed};
use crate::error::{ReelError, Result};
use crate::format::band::Band;
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
        self.put_owned_banded(key, payload, None)
    }

    /// The same put in a band, which places it beside what dies when it does
    ///
    /// The band is the caller's own window number: records carrying the same one go to
    /// the same tail, so the segment they fill can be reclaimed by unlinking it rather
    /// than by copying whatever outlived it. Nothing checks the number and nothing
    /// requires it to be right; a wrong band costs placement and nothing else.
    pub fn put_banded(&self, key: &RecordKey, payload: &[u8], band: Band) -> Result<()> {
        self.put_owned_banded(key, payload.to_vec(), Some(band))
    }

    /// Owned-payload put into a band, or into the unbanded tails where there is none
    pub fn put_owned_banded(
        &self,
        key: &RecordKey,
        payload: Vec<u8>,
        band: Option<Band>,
    ) -> Result<()> {
        let planned = self.plan_put(key, payload)?;
        // The tail owns the key it queues, and the index insert below needs it too.
        let committed = self.reel.put(
            key.clone(),
            planned.payload,
            planned.codec,
            Commit::PerRecord,
            band,
        )?;
        self.index
            .insert(key, committed.loc, committed.lsn, planned.carried)?;
        Ok(())
    }

    /// The same put awaited, for a caller with a runtime worker to protect
    ///
    /// The record reaches the device on this thread either way; what is awaited is
    /// admission and the sync.
    pub async fn put_owned_wait(&self, key: &RecordKey, payload: Vec<u8>) -> Result<()> {
        self.put_owned_banded_wait(key, payload, None).await
    }

    /// The awaited put in a band
    pub async fn put_owned_banded_wait(
        &self,
        key: &RecordKey,
        payload: Vec<u8>,
        band: Option<Band>,
    ) -> Result<()> {
        let planned = self.plan_put(key, payload)?;
        let committed = self
            .reel
            .put_wait(
                key.clone(),
                planned.payload,
                planned.codec,
                Commit::PerRecord,
                band,
            )
            .await?;
        self.index
            .insert(key, committed.loc, committed.lsn, planned.carried)?;
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
        let (payload, codec) = crate::append::codec::admit(
            self.index.codec_of(key.column),
            self.index.inline_max(key.column),
            payload,
        );
        // Taken before the tail takes the buffer, and taken on stored bytes, so a
        // compressed record carries what it stored.
        let carried = self.index.carry_capture(key.column, &payload);
        Ok(Planned {
            payload,
            codec,
            carried,
        })
    }

    /// Apply a batch of writes as one reservation, one write, and one sync
    ///
    /// A batch is one durability point: the sync is taken once the last record has
    /// landed, and the index moves only after it comes back clean.
    pub fn apply_batch(&self, writes: Vec<RecordWrite>) -> Result<()> {
        self.apply_batch_banded(writes, None)
    }

    /// The same batch placed in a band, since one tail takes all of it
    pub fn apply_batch_banded(&self, writes: Vec<RecordWrite>, band: Option<Band>) -> Result<()> {
        let Some((records, keys)) = self.plan_batch(writes)? else {
            return Ok(());
        };

        let committed = self.reel.write_batch(records, band)?;
        self.reel.sync_if_owed()?;
        self.publish_batch(&keys, &committed)
    }

    /// The same batch awaited, one submission and one durability point
    ///
    /// The plan and the publish are the blocking batch's own, so the only thing a
    /// row racing the doors sees is where the two waits went.
    pub async fn apply_batch_wait(&self, writes: Vec<RecordWrite>) -> Result<()> {
        self.apply_batch_banded_wait(writes, None).await
    }

    /// The awaited batch placed in a band
    pub async fn apply_batch_banded_wait(
        &self,
        writes: Vec<RecordWrite>,
        band: Option<Band>,
    ) -> Result<()> {
        let Some((records, keys)) = self.plan_batch(writes)? else {
            return Ok(());
        };

        let committed = self.reel.write_batch_wait(records, band).await?;
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
                    let (payload, codec) = crate::append::codec::admit(
                        self.index.codec_of(key.column),
                        self.index.inline_max(key.column),
                        payload,
                    );
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
            let carried = match &write {
                BatchWrite::Put(payload, _) => self.index.carry_capture(key.column, payload),
                BatchWrite::Delete | BatchWrite::DeleteRange(_) => None,
            };
            records.push(BatchRecord {
                key: key.clone(),
                write,
            });
            keys.push(BatchKey { key, op, carried });
        }
        if records.is_empty() {
            return Ok(None);
        }
        Ok(Some((records, keys)))
    }

    /// Move the index onto a batch that has landed, under the publish barrier
    fn publish_batch(&self, keys: &[BatchKey], committed: &[Committed]) -> Result<()> {
        // The publish moves the index one key at a time, so the barrier is what makes
        // a read spanning several keys land before it or after it, never inside it.
        // Only the map moves under that hold: settling a paged column's displaced key
        // reads a footer, and a reader waiting on the barrier must not be waiting on
        // the volume, so those are collected and run afterwards.
        let pages = self.index.residency().pages();
        // Built before the barrier is taken, so the hold costs only the move. A range
        // is held apart with the count of moves ahead of it, since the index applies a
        // run of key moves at a time and a cover is not one of them.
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
                    carried: planned.carried.clone(),
                    is_delete: matches!(op, KeyOp::Delete),
                }),
            }
        }

        let landed = self.index.publish_batch(&moves, &ranges);

        // One answer per key move, so the ranges are stepped over rather than paired.
        let mut answers = landed.iter();
        let mut displaced = Vec::new();
        for planned in keys {
            if matches!(planned.op, KeyOp::Range(_)) {
                continue;
            }
            let Some(mapped) = answers.next() else {
                break;
            };
            if pages && mapped.may_be_paged() {
                displaced.push(planned.key.clone());
            }
        }

        for key in displaced {
            self.index.settle_displaced(&key)?;
        }
        Ok(())
    }

    /// Append a tombstone and drop the key from the index
    pub fn delete(&self, key: &RecordKey) -> Result<()> {
        if self.is_read_only {
            return Err(read_only());
        }
        self.check_column(key)?;
        let committed = self.reel.delete(key.clone(), Commit::PerRecord)?;
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
