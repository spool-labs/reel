//! A batch: one claim in the write order and one group of rows, so it lands whole or not at all

use std::io::{Error, ErrorKind};
use std::sync::atomic::Ordering;

use reel_api::column_id::ColumnId;
use reel_api::store_error::StoreError;
use reel_api::units::{ByteCount, FileOffset, RowCount, VolumeId, WritePlace};
use reel_api::write_batch::{BatchOperation, WriteBatch};

use crate::engine::engine_sizing::GROUP_BYTES;
use crate::engine::engine_state::EngineState;
use crate::engine::volume::Volume;
use crate::engine::write_path::{ClaimRequest, INDEX_WAIT, check_value_size, check_writable, store_error};
use crate::guard::hold;
use crate::index::key_hash::hash_key;
use crate::index::write_index::{SHARD_COUNT, shard_of};
use crate::index::write_row::WriteRow;
use crate::index::write_shard::ShardNeed;
use crate::io::{Io, IoFile};
use crate::record::codec::{CHUNK_BYTES, Codec};
use crate::record::record_check::RecordIdentity;
use crate::record::record_encoder::RecordEncoder;
use crate::record::record_head::plain_length;
use crate::row_kind::RowKind;
use crate::rows::row_group::{GROUP_OVERHEAD_BYTES, RowEntry, RowGroupBuffer, row_size};

/// The most bytes of rows one group can carry
pub const MOST_GROUP_ROW_BYTES: usize = GROUP_BYTES.0 as usize - GROUP_OVERHEAD_BYTES;

/// What a batch asks of its volume and the write index, read before anything is written
#[derive(Clone, Copy, Debug)]
pub struct BatchPlan {
    pub volume: VolumeId,
    pub rows: RowCount,
    pub row_bytes: usize,
    pub deletes: u64,
    pub has_large_value: bool,
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

        let encoders = if plan.has_large_value { &self.large_encoders } else { &self.encoders };
        let bytes = encoders.with(|encoder| {
            self.batch_rows.with(|stage| {
                let bytes = self.measure_batch(volume, batch, encoder)?;
                self.hold_batch_room(&plan)?;
                let landed = self.land_batch(volume, batch, &plan, bytes, (encoder, stage));
                self.index.release_room(&plan.needs);
                landed.map(|()| bytes)
            })
        })?;
        self.after_write(volume, bytes)
    }

    /// Check every operation and add up the rows and room, or return nothing for an empty batch
    pub fn plan_batch(&self, batch: &WriteBatch) -> Result<Option<BatchPlan>, StoreError> {
        let mut plan: Option<BatchPlan> = None;
        for index in 0..batch.len() {
            let Some(operation) = batch.operation(index) else {
                continue;
            };
            let (column, key, value) = match operation {
                BatchOperation::Put { column, key, value } => (column, key, Some(value)),
                BatchOperation::Delete { column, key } => (column, key, None),
                BatchOperation::DeleteRange { .. } => {
                    return Err(StoreError::Io {
                        action: "writing a batch",
                        source: Error::new(ErrorKind::Unsupported, "range deletes aren't built yet"),
                    });
                },
            };

            let info = self.column(column)?;
            self.check_key(column, key)?;
            let plan = plan.get_or_insert(BatchPlan {
                volume: info.volume,
                rows: RowCount(0),
                row_bytes: 0,
                deletes: 0,
                has_large_value: false,
                needs: [ShardNeed::default(); SHARD_COUNT],
            });
            if info.volume != plan.volume {
                return Err(StoreError::BatchAcrossVolumes);
            }

            match value {
                Some(value) => {
                    check_value_size(&self.volumes[info.volume.0 as usize], value)?;
                    plan.has_large_value |= info.codec == Codec::Lz4 && value.len() > CHUNK_BYTES;
                },
                None => plan.deletes += 1,
            }
            plan.rows = RowCount(plan.rows.0 + 1);
            plan.row_bytes += row_size(key.len());
            plan.needs[shard_of(hash_key(column, key))].add(key.len());
        }

        if let Some(plan) = &plan {
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
            if let Some(BatchOperation::Put { column, key, value }) = batch.operation(index) {
                let length = self.record_length(column, key, value, encoder)?;
                bytes = ByteCount(bytes.0 + length.0);
            }
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
        match encoder.encode(&identity, info.codec, value) {
            Some(shape) => Ok(shape.total_length()),
            None => Err(StoreError::ValueTooLarge {
                length: ByteCount(value.len() as u64),
                limit: self.sizing.lz4_stored_limit,
            }),
        }
    }

    /// Hold index room for the whole batch, starting seals while there's none
    pub fn hold_batch_room(&self, plan: &BatchPlan) -> Result<(), StoreError> {
        let deadline = self.io.now().0.saturating_add(INDEX_WAIT.0);
        while !self.index.hold_room(&plan.needs) {
            for volume in &self.volumes {
                self.seal_fullest(volume);
            }
            if self.io.now().0 > deadline {
                return Err(StoreError::Io {
                    action: "waiting for the write index",
                    source: Error::other("no seal freed room for a batch"),
                });
            }
            self.io.pause();
        }
        Ok(())
    }

    /// Claim the batch's bytes and places at once, write its records, and only then show its rows to anyone
    pub fn land_batch(
        &self,
        volume: &Volume<Backend>,
        batch: &WriteBatch,
        plan: &BatchPlan,
        bytes: ByteCount,
        (encoder, stage): (&mut RecordEncoder, &mut RowGroupBuffer),
    ) -> Result<(), StoreError> {
        let request = ClaimRequest { bytes, rows: plan.rows };
        let claim = self.claim(volume, &request)?;
        let mut cursor = BatchCursor {
            offset: claim.offset,
            place: claim.first_place(),
        };

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
                    let Some(shape) = encoder.encode(&identity, info.codec, value) else {
                        return Err(StoreError::ValueTooLarge {
                            length: ByteCount(value.len() as u64),
                            limit: self.sizing.lz4_stored_limit,
                        });
                    };
                    let parts = encoder.parts(&shape, value);
                    if let Err(source) = claim.data.write_at(cursor.offset, &parts) {
                        return Err(store_error(volume, "writing a record", source));
                    }
                    cursor.row(column, key, shape.total_length(), RowKind::Value)
                },
                BatchOperation::Delete { column, key } => cursor.row(column, key, ByteCount(0), RowKind::Grave),
                BatchOperation::DeleteRange { .. } => continue,
            };
            let is_staged = stage.push(&row);
            debug_assert!(is_staged);
        }

        let (slot, segment) = (claim.slot, claim.segment);
        let size = stage.row_bytes();
        self.reserve_row(slot, size)?;
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
            queue.reserved -= size;
            // the reservation holds this room, whichever group is filling by now
            let is_appended = queue.filling.append(stage);
            debug_assert!(is_appended);
        }
        self.wake_row_job();
        self.deletes.fetch_add(plan.deletes, Ordering::Relaxed);
        claim.land();
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
