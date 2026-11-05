//! A batch: one claim in the write order and one group of rows, so it lands whole or not at all

use std::io::{Error, ErrorKind, IoSlice};
use std::slice::Iter;
use std::sync::atomic::Ordering;

use reel_api::column_id::ColumnId;
use reel_api::store_error::StoreError;
use reel_api::units::{ByteCount, FileOffset, RowCount, VolumeId, WritePlace};
use reel_api::write_batch::{BatchOperation, WriteBatch};
use xxhash_rust::xxh3::Xxh3;

use crate::engine::claim::Claim;
use crate::engine::engine_sizing::GROUP_BYTES;
use crate::engine::engine_state::{EngineState, IndexWait};
use crate::engine::volume::Volume;
use crate::engine::write_path::{ClaimRequest, INDEX_WAIT, check_value_size, check_writable, store_error};
use crate::guard::hold;
use crate::index::key_hash::hash_key;
use crate::index::open_slots::OpenSlot;
use crate::index::write_index::{SHARD_COUNT, shard_of};
use crate::index::write_row::WriteRow;
use crate::index::write_shard::ShardNeed;
use crate::io::{Io, IoFile};
use crate::record::codec::Codec;
use crate::record::record_check::RecordIdentity;
use crate::record::record_encoder::{PlainRecord, RecordEncoder};
use crate::record::record_head::plain_length;
use crate::row_kind::RowKind;
use crate::record::record_check::RecordKey;
use crate::rows::row_group::{GROUP_OVERHEAD_BYTES, GroupHead, RowEntry, RowGroupBuffer, row_size};
use crate::segment::range_covers::Cover;
use crate::segment::segment_tail::SegmentNumber;

/// The most bytes of rows one group's buffer can carry
pub const MOST_BUFFERED_ROW_BYTES: usize = GROUP_BYTES.0 as usize - GROUP_OVERHEAD_BYTES;
/// The most bytes of rows a group's length field can reach
pub const MOST_GROUP_ROW_BYTES: usize = u32::MAX as usize - GROUP_OVERHEAD_BYTES;

/// The buffers a batch works in
pub struct BatchBuffers<'buffers> {
    pub encoder: &'buffers mut RecordEncoder,
    pub stage: &'buffers mut RowGroupBuffer,
    pub records: &'buffers mut [u8],
    pub lengths: &'buffers mut [u32],
}

/// What a batch asks of its volume and the write index, read before anything is written
#[derive(Clone, Copy, Debug)]
pub struct BatchPlan {
    pub volume: VolumeId,
    pub rows: RowCount,
    pub row_bytes: usize,
    pub deletes: u64,
    pub covers: usize,
    pub needs: [ShardNeed; SHARD_COUNT],
}

impl<Backend: Io> EngineState<Backend> {
    /// Apply a batch whole, in batch order
    pub fn write_batch(&self, batch: &WriteBatch) -> Result<(), StoreError> {
        let Some(plan) = self.plan_batch(batch)? else {
            return Ok(());
        };
        let volume = &self.volumes[plan.volume.0 as usize];
        check_writable(volume)?;
        if !volume.covers.hold(plan.covers) {
            return Err(StoreError::RangeDeletesFull { volume: volume.id });
        }

        let landed = self.encoders.with(|encoder| {
            self.batch_rows.with(|stage| self.batch_records.with(|records| {
                let bytes = self.measure_batch(volume, batch, encoder)?;
                self.hold_batch_room(&plan)?;
                let landed = if plan.row_bytes > MOST_BUFFERED_ROW_BYTES {
                    self.batch_lengths.with(|lengths| {
                        let buffers = BatchBuffers { encoder, stage, records, lengths };
                        self.land_batch(volume, batch, &plan, bytes, buffers)
                    })
                } else {
                    let buffers = BatchBuffers { encoder, stage, records, lengths: &mut [] };
                    self.land_batch(volume, batch, &plan, bytes, buffers)
                };
                self.index.release_room(&plan.needs);
                landed.map(|()| bytes)
            }))
        });
        let bytes = landed.inspect_err(|_error| volume.covers.release(plan.covers))?;
        self.after_write(volume, bytes)
    }

    /// Check every operation and add up the rows and room, or return nothing for an empty batch
    pub fn plan_batch(&self, batch: &WriteBatch) -> Result<Option<BatchPlan>, StoreError> {
        let mut plan: Option<BatchPlan> = None;
        for index in 0..batch.len() {
            let Some(operation) = batch.operation(index) else {
                continue;
            };
            let (column, key, value, end) = match operation {
                BatchOperation::Put { column, key, value } => (column, key, Some(value), None),
                BatchOperation::Delete { column, key } => (column, key, None, None),
                // a range with nothing in it writes nothing, as the single call does
                BatchOperation::DeleteRange { start, end, .. } if start >= end => continue,
                BatchOperation::DeleteRange { column, start, end } => (column, start, None, Some(end)),
            };

            let info = self.column(column)?;
            self.check_key(column, key)?;
            let plan = plan.get_or_insert(BatchPlan {
                volume: info.volume,
                rows: RowCount(0),
                row_bytes: 0,
                deletes: 0,
                covers: 0,
                needs: [ShardNeed::default(); SHARD_COUNT],
            });
            if info.volume != plan.volume {
                return Err(StoreError::BatchAcrossVolumes);
            }

            match (value, end) {
                (Some(value), _no_end) => {
                    check_value_size(&self.volumes[info.volume.0 as usize], value)?;
                },
                (None, Some(end)) => {
                    self.check_key(column, end)?;
                    plan.covers += 1;
                },
                (None, None) => plan.deletes += 1,
            }
            plan.rows = RowCount(plan.rows.0 + 1);
            plan.row_bytes += row_size(key.len());
            plan.needs[shard_of(hash_key(column, key))].add(key.len());
        }

        if let Some(plan) = &plan {
            let volume = &self.volumes[plan.volume.0 as usize];
            if plan.rows > volume.segment_rows {
                return Err(StoreError::BatchTooManyRows {
                    rows: plan.rows,
                    limit: volume.segment_rows,
                });
            }
            let limit = ByteCount(MOST_GROUP_ROW_BYTES as u64);
            let length = ByteCount(plan.row_bytes as u64);
            if length > limit || !self.index.could_hold(&plan.needs) {
                return Err(StoreError::BatchTooLarge { length, limit });
            }
        }
        Ok(plan)
    }

    /// Add up the bytes the batch's records take, and refuse a batch no segment could hold
    pub fn measure_batch(
        &self,
        volume: &Volume<Backend>,
        batch: &WriteBatch,
        encoder: &mut RecordEncoder,
    ) -> Result<ByteCount, StoreError> {
        let mut bytes = ByteCount(0);
        for index in 0..batch.len() {
            let length = match batch.operation(index) {
                Some(BatchOperation::Put { column, key, value }) => self.record_length(column, key, value, encoder)?,
                Some(BatchOperation::DeleteRange { start, end, .. }) if start < end => {
                    plain_length(ByteCount(end.len() as u64))
                },
                Some(BatchOperation::Delete { .. } | BatchOperation::DeleteRange { .. }) | None => ByteCount(0),
            };
            bytes = ByteCount(bytes.0 + length.0);
        }

        if bytes > volume.segment_bytes {
            return Err(StoreError::BatchTooLarge {
                length: bytes,
                limit: volume.segment_bytes,
            });
        }
        Ok(bytes)
    }

    /// Count a value's stored bytes, which only a compressed column has to encode to learn
    pub fn record_length(
        &self,
        column: ColumnId,
        key: &[u8],
        value: &[u8],
        encoder: &mut RecordEncoder,
    ) -> Result<ByteCount, StoreError> {
        let info = self.column(column)?;
        if info.codec == Codec::None {
            return Ok(plain_length(ByteCount(value.len() as u64)));
        }

        let identity = RecordIdentity {
            record_key: self.record_key,
            column,
            key,
            kind: RowKind::Value,
            place: WritePlace(0),
        };
        let shape = self.encode_value(encoder, &identity, info.codec, value)?;
        encoder.give_shares(&self.stored_shares);
        Ok(shape.total_length())
    }

    /// Put a batch's range deletes in the pool, each at its operation's place, once its rows can't fail
    pub fn add_covers(&self, batch: &WriteBatch, plan: &BatchPlan, first: WritePlace, segment: SegmentNumber) {
        if plan.covers == 0 {
            return;
        }
        let volume = &self.volumes[plan.volume.0 as usize];
        let mut place_cursor = first;
        for index in 0..batch.len() {
            let Some(operation) = batch.operation(index) else {
                continue;
            };
            if let BatchOperation::DeleteRange { start, end, .. } = operation
                && start >= end
            {
                continue;
            }
            let place = place_cursor;
            place_cursor = WritePlace(place_cursor.0 + 1);
            if let BatchOperation::DeleteRange { column, start, end } = operation {
                volume.covers.add_held(&Cover {
                    column,
                    start,
                    end,
                    place,
                    segment,
                });
            }
        }
    }

    /// Drop the older covers inside a batch's range deletes, once the batch is published
    pub fn drop_covered(&self, batch: &WriteBatch, plan: &BatchPlan, first: WritePlace, segment: SegmentNumber) {
        if plan.covers == 0 {
            return;
        }
        let volume = &self.volumes[plan.volume.0 as usize];
        let mut place_cursor = first;
        for index in 0..batch.len() {
            let Some(operation) = batch.operation(index) else {
                continue;
            };
            if let BatchOperation::DeleteRange { start, end, .. } = operation
                && start >= end
            {
                continue;
            }
            let place = place_cursor;
            place_cursor = WritePlace(place_cursor.0 + 1);
            if let BatchOperation::DeleteRange { column, start, end } = operation {
                self.drop_covers_inside(volume, &Cover {
                    column,
                    start,
                    end,
                    place,
                    segment,
                });
            }
        }
    }

    /// Hold index room for the whole batch, starting seals while there's none
    pub fn hold_batch_room(&self, plan: &BatchPlan) -> Result<(), StoreError> {
        if self.index.hold_room(&plan.needs) {
            return Ok(());
        }

        let started = self.io.now();
        self.index_waits.fetch_add(1, Ordering::Relaxed);
        let _waiting = IndexWait::new(&self.index_waiters);
        let deadline = started.0.saturating_add(INDEX_WAIT.0);
        loop {
            for volume in &self.volumes {
                self.seal_fullest(volume);
            }
            if self.io.now().0 > deadline {
                return Err(StoreError::Io {
                    action: "waiting for the write index",
                    source: Error::from(ErrorKind::TimedOut),
                });
            }
            self.io.pause();
            if self.index.hold_room(&plan.needs) {
                let waited = self.io.now().0.saturating_sub(started.0);
                self.longest_index_wait.fetch_max(waited, Ordering::Relaxed);
                return Ok(());
            }
        }
    }

    /// Claim the batch's bytes and places at once, write its records, and only then show its rows to anyone
    pub fn land_batch(
        &self,
        volume: &Volume<Backend>,
        batch: &WriteBatch,
        plan: &BatchPlan,
        bytes: ByteCount,
        buffers: BatchBuffers,
    ) -> Result<(), StoreError> {
        let BatchBuffers { encoder, stage, records, lengths } = buffers;
        let is_long = plan.row_bytes > MOST_BUFFERED_ROW_BYTES;
        let request = ClaimRequest {
            bytes,
            rows: plan.rows,
            is_alone: is_long,
            has_records: bytes.0 > 0,
            covers: plan.covers,
        };
        // a long group goes out under its own turn, and a short one needs room in the group filling
        let (claim, room) = if is_long {
            (self.claim(volume, &request)?, None)
        } else {
            let (claim, room) = self.claim_with_room(volume, &request, plan.row_bytes)?;
            (claim, Some(room))
        };
        let start = BatchCursor {
            offset: claim.offset,
            place: claim.first_place(),
        };
        let mut cursor = start;

        let mut kept_lengths = lengths.iter_mut();
        let mut gathered = GatheredRecords::new(&claim.data, records, claim.offset);
        stage.clear();
        for index in 0..batch.len() {
            let Some(operation) = batch.operation(index) else {
                continue;
            };
            let row = match operation {
                BatchOperation::Put { column, key, value } => {
                    let info = self.column(column)?;
                    let identity = RecordIdentity {
                        record_key: self.record_key,
                        column,
                        key,
                        kind: RowKind::Value,
                        place: cursor.place,
                    };
                    let shape = self.encode_value(encoder, &identity, info.codec, value)?;
                    let written = if shape.table_length == 0 {
                        gathered.add(&encoder.parts(&shape, value))
                    } else {
                        gathered.send().and_then(|()| {
                            gathered.skip_to(FileOffset(cursor.offset.0 + shape.total_length().0));
                            encoder.write_to(&claim.data, cursor.offset, &shape, value)
                        })
                    };
                    encoder.give_shares(&self.stored_shares);
                    if let Err(source) = written {
                        return Err(store_error(volume, "writing a record", source));
                    }
                    cursor.row(column, key, shape.total_length(), RowKind::Value)
                },
                BatchOperation::Delete { column, key } => cursor.row(column, key, ByteCount(0), RowKind::Grave),
                BatchOperation::DeleteRange { start, end, .. } if start >= end => continue,
                BatchOperation::DeleteRange { column, start, end } => {
                    let identity = RecordIdentity {
                        record_key: self.record_key,
                        column,
                        key: start,
                        kind: RowKind::RangeDelete,
                        place: WritePlace(0),
                    };
                    let head = PlainRecord::new(&identity, end).head(self.record_key, cursor.place);
                    if let Err(source) = gathered.add(&[IoSlice::new(&head), IoSlice::new(end)]) {
                        return Err(store_error(volume, "writing a record", source));
                    }
                    let length = plain_length(ByteCount(end.len() as u64));
                    cursor.row(column, start, length, RowKind::RangeDelete)
                },
            };
            if is_long {
                if let Some(kept) = kept_lengths.next() {
                    *kept = row.length.0 as u32;
                }
            } else {
                let is_staged = stage.push(&row);
                debug_assert!(is_staged);
            }
        }

        if let Err(source) = gathered.send() {
            return Err(store_error(volume, "writing a record", source));
        }

        if is_long {
            return self.land_long_group(plan, claim, BatchRows::new(batch, start, lengths), stage);
        }

        let (slot, segment) = (claim.slot, claim.segment);
        for row in stage.walk() {
            let write_row = WriteRow {
                place: row.place,
                offset: row.offset,
                length: row.length,
                kind: row.kind,
                slot,
                segment,
                has_newer_sealed: false,
            };
            let is_held = self.index.insert_held(row.column, row.key, &write_row);
            debug_assert!(is_held);
        }
        {
            let mut queue = hold(&self.queues[slot.0 as usize]);
            // the reservation holds this room, whichever group is filling by now
            let is_appended = queue.filling.append(stage);
            debug_assert!(is_appended);
        }
        drop(room);
        self.wake_row_job();
        self.add_covers(batch, plan, start.place, segment);
        self.deletes.fetch_add(plan.deletes, Ordering::Relaxed);
        claim.land();
        self.drop_covered(batch, plan, start.place, segment);
        Ok(())
    }

    /// Land rows past one buffer: send them as one group under the writing turn, then put them in the index, which
    /// can't fail. A failed write leaves a torn group on disk, nothing in the index and nothing for a read
    pub fn land_long_group(
        &self,
        plan: &BatchPlan,
        claim: Claim<'_, Backend>,
        rows_of_batch: BatchRows<'_>,
        stage: &mut RowGroupBuffer,
    ) -> Result<(), StoreError> {
        let (slot, segment) = (claim.slot, claim.segment);
        let Some((pin, rows)) = self.pin_rows(slot) else {
            return Err(StoreError::Io {
                action: "writing rows",
                source: Error::other("a claimed segment has no rows file"),
            });
        };

        let total = ByteCount((plan.row_bytes + GROUP_OVERHEAD_BYTES) as u64);
        self.check_rows_room(claim.volume, slot, total)?;

        // no other writer's rows wait behind the turn: new claims go elsewhere, and these go out first
        self.wait_for_other_claims(claim.volume, slot);
        let _sent = self.write_out(slot);
        let started = self.io.now();
        self.take_writing_turn(slot);
        let (batch, first_place) = (rows_of_batch.batch, rows_of_batch.cursor.place);
        let sent = self.send_long_group(slot, &rows, plan, rows_of_batch.clone(), stage);
        hold(&self.queues[slot.0 as usize]).is_writing = false;
        let held = self.io.now().0.saturating_sub(started.0);
        self.longest_turn.fetch_max(held, Ordering::Relaxed);
        drop(pin);
        if let Err(source) = sent {
            // the row job passes over a full disk alone, and a rows file that won't take rows stops the volume
            if source.kind() != ErrorKind::StorageFull {
                claim.volume.fail();
            }
            return Err(store_error(claim.volume, "writing rows", source));
        }

        for row in rows_of_batch {
            let write_row = WriteRow {
                place: row.place,
                offset: row.offset,
                length: row.length,
                kind: row.kind,
                slot,
                segment,
                has_newer_sealed: false,
            };
            let is_held = self.index.insert_held(row.column, row.key, &write_row);
            debug_assert!(is_held);
        }
        self.add_covers(batch, plan, first_place, segment);
        self.deletes.fetch_add(plan.deletes, Ordering::Relaxed);
        claim.land();
        self.drop_covered(batch, plan, first_place, segment);
        Ok(())
    }

    /// Refuse a group its root has no room for before any of it goes out
    pub fn check_rows_room(&self, volume: &Volume<Backend>, slot: OpenSlot, total: ByteCount) -> Result<(), StoreError> {
        let root = hold(&volume.claims).open.iter().find(|segment| segment.slot == slot).map(|segment| segment.root);
        let Some(root) = root else {
            return Ok(());
        };
        let free = self
            .io
            .free_space(&volume.roots[root.0 as usize])
            .map_err(|source| store_error(volume, "reading free space", source))?;
        if free < total {
            return Err(StoreError::VolumeFull { volume: volume.id });
        }
        Ok(())
    }

    /// Wait until a slot's only claim is the caller's
    pub fn wait_for_other_claims(&self, volume: &Volume<Backend>, slot: OpenSlot) {
        loop {
            let is_alone = hold(&volume.claims).open.iter().any(|segment| segment.slot == slot && segment.in_flight == 1);
            if is_alone {
                return;
            }
            self.io.pause();
        }
    }

    /// Send a batch's rows as one group, in pieces through the stage. Only the holder of the writing turn may
    pub fn send_long_group(
        &self,
        slot: OpenSlot,
        rows: &Backend::File,
        plan: &BatchPlan,
        batch_rows: BatchRows<'_>,
        stage: &mut RowGroupBuffer,
    ) -> std::io::Result<()> {
        let queue = &self.queues[slot.0 as usize];
        let (rows_end, synced_through) = {
            let guard = hold(queue);
            (guard.rows_end, guard.synced_through)
        };
        let total = ByteCount((plan.row_bytes + GROUP_OVERHEAD_BYTES) as u64);
        let group = LongGroup::new(rows, rows_end, self.record_key);

        stage.clear();
        stage.set_head(&GroupHead {
            row_count: plan.rows,
            total_length: total,
            synced_through,
        });
        let sent = send_pieces(group, batch_rows, stage);
        // a failed write may still have landed, and a whole group left behind would bring the batch back at open
        if sent.is_err() && rows.truncate(ByteCount(rows_end.0)).is_err() {
            return Err(Error::other("a failed group can't be cut from its rows file"));
        }
        sent?;
        hold(queue).rows_end = FileOffset(rows_end.0 + total.0);
        Ok(())
    }
}

/// Send a batch's rows through the stage, a buffer at a time
pub fn send_pieces<File: IoFile>(
    mut group: LongGroup<'_, File>,
    batch_rows: BatchRows<'_>,
    stage: &mut RowGroupBuffer,
) -> std::io::Result<()> {
    for row in batch_rows {
        if !stage.push(&row) {
            group.send(stage)?;
            stage.clear();
            let is_staged = stage.push(&row);
            debug_assert!(is_staged);
        }
    }
    group.finish(stage)
}

/// A long batch's rows, built from its operations and their record lengths
#[derive(Clone, Debug)]
pub struct BatchRows<'batch> {
    batch: &'batch WriteBatch,
    index: usize,
    cursor: BatchCursor,
    lengths: Iter<'batch, u32>,
}

impl<'batch> BatchRows<'batch> {
    /// Start at the batch's first row
    pub fn new(batch: &'batch WriteBatch, cursor: BatchCursor, lengths: &'batch [u32]) -> Self {
        Self {
            batch,
            index: 0,
            cursor,
            lengths: lengths.iter(),
        }
    }
}

impl<'batch> Iterator for BatchRows<'batch> {
    type Item = RowEntry<'batch>;

    fn next(&mut self) -> Option<RowEntry<'batch>> {
        loop {
            let operation = self.batch.operation(self.index)?;
            self.index += 1;
            if let BatchOperation::DeleteRange { start, end, .. } = operation
                && start >= end
            {
                continue;
            }

            let length = ByteCount(u64::from(*self.lengths.next()?));
            return match operation {
                BatchOperation::Put { column, key, .. } => Some(self.cursor.row(column, key, length, RowKind::Value)),
                BatchOperation::Delete { column, key } => Some(self.cursor.row(column, key, length, RowKind::Grave)),
                BatchOperation::DeleteRange { column, start, .. } => {
                    Some(self.cursor.row(column, start, length, RowKind::RangeDelete))
                },
            };
        }
    }
}

/// A batch's one-chunk records on their way out, gathered so they share a write call
pub struct GatheredRecords<'work, File: IoFile> {
    data: &'work File,
    buffer: &'work mut [u8],
    start: FileOffset,
    used: usize,
}

impl<'work, File: IoFile> GatheredRecords<'work, File> {
    /// Start gathering at an offset
    pub fn new(data: &'work File, buffer: &'work mut [u8], start: FileOffset) -> Self {
        Self {
            data,
            buffer,
            start,
            used: 0,
        }
    }

    /// Copy a record in, sending what's gathered first when it doesn't fit
    pub fn add(&mut self, parts: &[IoSlice]) -> std::io::Result<()> {
        let length: usize = parts.iter().map(|part| part.len()).sum();
        if self.used + length > self.buffer.len() {
            self.send()?;
        }
        for part in parts {
            self.buffer[self.used..self.used + part.len()].copy_from_slice(part);
            self.used += part.len();
        }
        Ok(())
    }

    /// Send what's gathered
    pub fn send(&mut self) -> std::io::Result<()> {
        if self.used == 0 {
            return Ok(());
        }
        self.data.write_at(self.start, &[IoSlice::new(&self.buffer[..self.used])])?;
        self.start = FileOffset(self.start.0 + self.used as u64);
        self.used = 0;
        Ok(())
    }

    /// Go on past a record written on its own
    pub fn skip_to(&mut self, start: FileOffset) {
        self.start = start;
    }
}

/// A group longer than its buffer, on its way to the rows file in pieces under one running hash
pub struct LongGroup<'file, File: IoFile> {
    pub rows: &'file File,
    pub at: FileOffset,
    pub written: u64,
    pub sum: Xxh3,
}

impl<'file, File: IoFile> LongGroup<'file, File> {
    /// Start a group at an offset
    pub fn new(rows: &'file File, at: FileOffset, record_key: RecordKey) -> Self {
        Self {
            rows,
            at,
            written: 0,
            sum: Xxh3::with_seed(record_key.0),
        }
    }

    /// Send what the buffer holds, the header with the first piece
    pub fn send(&mut self, stage: &RowGroupBuffer) -> std::io::Result<()> {
        let piece = if self.written == 0 { stage.with_head() } else { stage.rows() };
        self.sum.update(piece);
        self.write(piece)
    }

    /// Send the last piece and the checksum
    pub fn finish(mut self, stage: &RowGroupBuffer) -> std::io::Result<()> {
        self.send(stage)?;
        let sum = self.sum.digest().to_le_bytes();
        self.write(&sum)
    }

    /// Write bytes where the group has reached
    pub fn write(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        self.rows.write_at(FileOffset(self.at.0 + self.written), &[IoSlice::new(bytes)])?;
        self.written += bytes.len() as u64;
        Ok(())
    }
}

/// Where a batch's next row goes
#[derive(Clone, Copy, Debug)]
pub struct BatchCursor {
    pub offset: FileOffset,
    pub place: WritePlace,
}

impl BatchCursor {
    /// Build the next row and step past it
    pub fn row<'key>(
        &mut self,
        column: ColumnId,
        key: &'key [u8],
        length: ByteCount,
        kind: RowKind,
    ) -> RowEntry<'key> {
        let row = RowEntry {
            column,
            key,
            place: self.place,
            offset: self.offset,
            length,
            kind,
        };
        self.offset = FileOffset(self.offset.0 + length.0);
        self.place = WritePlace(self.place.0 + 1);
        row
    }
}
