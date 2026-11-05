//! Puts, deletes and syncs

use std::io::{Error, ErrorKind, IoSlice};
use std::mem;
use std::sync::atomic::Ordering;

use reel_api::column_id::ColumnId;
use reel_api::store_error::StoreError;
use reel_api::units::{ByteCount, FileOffset, RootId, RowCount, WritePlace};

use crate::config::SyncPolicy;
use crate::engine::claim::Claim;
use crate::engine::engine_sizing::OPEN_PER_ROOT;
use crate::engine::engine_state::EngineState;
use crate::engine::open_segment::{OpenSegment, SegmentPhase, data_path, rows_path};
use crate::engine::volume::{ClaimState, Volume};
use crate::guard::hold;
use crate::index::key_hash::hash_key;
use crate::index::open_slots::OpenSlot;
use crate::index::write_row::WriteRow;
use crate::io::{Io, IoFile, IoTask};
use crate::record::codec::{CHUNK_BYTES, Codec};
use crate::record::record_check::RecordIdentity;
use crate::row_kind::RowKind;
use crate::rows::row_group::RowEntry;

/// In-flight writes an open segment takes before the volume opens another
pub const WRITES_PER_OPEN: u32 = 2;

/// What a claim asks for
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClaimRequest {
    pub bytes: ByteCount,
    pub rows: RowCount,
}

/// Turn a backend failure into a store error, and a full disk into the volume's own error
pub fn store_error<Backend: Io>(volume: &Volume<Backend>, action: &'static str, source: Error) -> StoreError {
    if source.kind() == ErrorKind::StorageFull {
        return StoreError::VolumeFull { volume: volume.id };
    }
    StoreError::Io { action, source }
}

impl<Backend: Io> EngineState<Backend> {
    /// Write a value
    pub fn write_value(&self, column: ColumnId, key: &[u8], value: &[u8]) -> Result<(), StoreError> {
        let info = self.column(column)?;
        self.check_key(column, key)?;
        let volume = &self.volumes[info.volume.0 as usize];
        check_value_size(volume, value, info.codec, self.sizing.lz4_stored_limit)?;
        check_writable(volume)?;

        let identity = RecordIdentity {
            record_key: self.record_key,
            column,
            key,
            kind: RowKind::Value,
        };
        let is_large = info.codec == Codec::Lz4 && value.len() > CHUNK_BYTES;
        let hint = hash_key(column, key).0 as usize;
        let mut encoder = if is_large {
            self.large_encoders.take(hint)
        } else {
            self.encoders.take(hint)
        };
        let Some(shape) = encoder.encode(&identity, info.codec, value) else {
            return Err(StoreError::ValueTooLarge {
                length: ByteCount(value.len() as u64),
                limit: self.sizing.lz4_stored_limit,
            });
        };

        let length = shape.total_length();
        let request = ClaimRequest {
            bytes: length,
            rows: RowCount(1),
        };
        let claim = self.claim(volume, &request)?;
        let parts = encoder.parts(&shape, value);
        if let Err(source) = claim.data.write_at(claim.offset, &parts) {
            return Err(store_error(volume, "writing a record", source));
        }
        drop(encoder);

        let row = WriteRow {
            place: claim.first_place(),
            offset: claim.offset,
            length,
            kind: RowKind::Value,
            slot: claim.slot,
        };
        self.land_row(column, key, &row)?;
        claim.land();
        self.after_write(volume, length)
    }

    /// Write a grave, unless the key's newest row in the index already is one
    pub fn write_grave(&self, column: ColumnId, key: &[u8]) -> Result<(), StoreError> {
        let info = self.column(column)?;
        self.check_key(column, key)?;
        let volume = &self.volumes[info.volume.0 as usize];
        if volume.has_failed() {
            return Err(StoreError::VolumeFailed { volume: volume.id });
        }
        if self.index.is_newest_grave(column, key, self.read_point(), volume.view()) {
            return Ok(());
        }

        let request = ClaimRequest {
            bytes: ByteCount(0),
            rows: RowCount(1),
        };
        let claim = self.claim(volume, &request)?;
        let row = WriteRow {
            place: claim.first_place(),
            offset: claim.offset,
            length: ByteCount(0),
            kind: RowKind::Grave,
            slot: claim.slot,
        };
        self.land_row(column, key, &row)?;
        claim.land();
        self.deletes.fetch_add(1, Ordering::Relaxed);
        self.after_write(volume, ByteCount(0))
    }

    /// Refuse a key of the wrong width
    pub fn check_key(&self, column: ColumnId, key: &[u8]) -> Result<(), StoreError> {
        let info = self.column(column)?;
        if info.key_shape.accepts(key.len()) {
            return Ok(());
        }
        Err(StoreError::KeyWidth {
            column,
            length: ByteCount(key.len() as u64),
        })
    }

    /// Put a row in the write index and the rows queue, waiting for a seal when either is full
    pub fn land_row(&self, column: ColumnId, key: &[u8], row: &WriteRow) -> Result<(), StoreError> {
        while !self.index.insert(column, key, row) {
            self.io.pause();
        }

        let entry = RowEntry {
            column,
            key,
            place: row.place,
            offset: row.offset,
            length: row.length,
            kind: row.kind,
        };
        let queue = &self.queues[row.slot.0 as usize];
        loop {
            if hold(queue).filling.push(&entry) {
                if !self.rows_waiting.swap(true, Ordering::AcqRel)
                    && let Some(task) = self.row_task.get()
                {
                    task.wake();
                }
                return Ok(());
            }
            self.write_out(row.slot).map_err(|source| StoreError::Io {
                action: "writing rows",
                source,
            })?;
            self.io.pause();
        }
    }

    /// Claim bytes and places together, opening a segment first when none has room
    pub fn claim<'state>(
        &'state self,
        volume: &'state Volume<Backend>,
        request: &ClaimRequest,
    ) -> Result<Claim<'state, Backend>, StoreError> {
        loop {
            let mut claims = hold(&volume.claims);
            if let Some(index) = pick_segment(&claims, volume, request) {
                let segment = &mut claims.open[index];
                let ticket = self.order.claim(request.rows);
                let offset = segment.write_end;
                segment.write_end = FileOffset(offset.0 + request.bytes.0);
                segment.row_count = RowCount(segment.row_count.0 + request.rows.0);
                segment.in_flight += 1;
                segment.lowest_place = segment.lowest_place.min(ticket.first);
                let data = segment.data.clone();
                let slot = segment.slot;
                self.slot_files[slot.0 as usize].pins.fetch_add(1, Ordering::AcqRel);
                self.mark_full_segments(&mut claims, volume);
                return Ok(Claim {
                    state: self,
                    volume,
                    slot,
                    data,
                    offset,
                    ticket,
                    is_landed: false,
                });
            }

            if claims.is_creating {
                drop(claims);
                self.io.pause();
                continue;
            }
            claims.is_creating = true;
            let root = claims.next_root;
            claims.next_root = (root + 1) % volume.roots.len();
            drop(claims);

            let created = self.create_segment(volume, RootId(root as u8), request);
            let mut claims = hold(&volume.claims);
            claims.is_creating = false;
            match created {
                Ok(Some(segment)) => {
                    volume.open_slot(segment.slot);
                    claims.open.push(segment);
                },
                Ok(None) => {
                    drop(claims);
                    self.io.pause();
                },
                Err(error) => return Err(error),
            }
        }
    }

    /// Start seals when the open segments together pass their limits
    pub fn mark_full_segments(&self, claims: &mut ClaimState<Backend::File>, volume: &Volume<Backend>) {
        let mut rows = 0;
        let mut bytes = 0;
        let mut fullest: Option<usize> = None;
        for (index, segment) in claims.open.iter().enumerate() {
            if segment.phase != SegmentPhase::Taking {
                continue;
            }
            rows += segment.row_count.0;
            bytes += segment.write_end.0;
            let is_fuller = match fullest {
                Some(best) => segment.write_end > claims.open[best].write_end,
                None => true,
            };
            if is_fuller {
                fullest = Some(index);
            }
        }

        if (rows >= volume.segment_rows.0 || bytes >= volume.segment_bytes.0)
            && let Some(index) = fullest
        {
            claims.open[index].phase = SegmentPhase::Sealing;
            if let Some(task) = self.seal_tasks[volume.id.0 as usize].get() {
                task.wake();
            }
        }
    }

    /// Create a segment's files outside every lock, or return nothing when no slot is free yet
    pub fn create_segment(
        &self,
        volume: &Volume<Backend>,
        root: RootId,
        request: &ClaimRequest,
    ) -> Result<Option<OpenSegment<Backend::File>>, StoreError> {
        let path = &volume.roots[root.0 as usize];
        let free = self
            .io
            .free_space(path)
            .map_err(|source| store_error(volume, "reading free space", source))?;
        if request.bytes.0 > 0 && free.0 < volume.reserve.0 + volume.segment_bytes.0 {
            return Err(StoreError::VolumeFull { volume: volume.id });
        }

        let Some(slot) = self.take_slot() else {
            return Ok(None);
        };
        let number = self.next_segment_number()?;
        let reserved = ByteCount(volume.segment_bytes.0.max(request.bytes.0));
        let reserved = if request.bytes.0 > 0 { reserved } else { ByteCount(0) };

        let data = self
            .io
            .create_file(&data_path(path, number), reserved)
            .map_err(|source| store_error(volume, "creating a segment", source));
        let data = match data {
            Ok(data) => data,
            Err(error) => {
                self.free_slot(slot);
                return Err(error);
            },
        };
        let rows = self
            .io
            .create_file(&rows_path(path, number), ByteCount(0))
            .map_err(|source| store_error(volume, "creating a rows file", source))?;
        self.io
            .sync_directory(path)
            .map_err(|source| store_error(volume, "syncing a root", source))?;

        hold(&self.queues[slot.0 as usize]).reset(FileOffset(0), FileOffset(0));
        self.slot_files[slot.0 as usize].set(Some(data.clone()));
        Ok(Some(OpenSegment {
            number,
            slot,
            root,
            data,
            rows,
            phase: SegmentPhase::Taking,
            write_end: FileOffset(0),
            row_count: RowCount(0),
            in_flight: 0,
            lowest_place: WritePlace(u64::MAX),
        }))
    }

    /// Run the volume's sync policy after a write
    pub fn after_write(&self, volume: &Volume<Backend>, length: ByteCount) -> Result<(), StoreError> {
        match volume.policy {
            SyncPolicy::EveryWrite => self.sync_volume(volume),
            SyncPolicy::EveryBytes(limit) => {
                let is_due = {
                    let mut claims = hold(&volume.claims);
                    claims.bytes_since_sync = ByteCount(claims.bytes_since_sync.0 + length.0);
                    if claims.bytes_since_sync >= limit {
                        claims.bytes_since_sync = ByteCount(0);
                        true
                    } else {
                        false
                    }
                };
                if is_due {
                    return self.sync_volume(volume);
                }
                Ok(())
            },
            SyncPolicy::Never => Ok(()),
        }
    }

    /// Make every returned write on a volume durable, sharing one sync with writers waiting beside it
    pub fn sync_volume(&self, volume: &Volume<Backend>) -> Result<(), StoreError> {
        let wanted = {
            let mut sync = hold(&volume.sync);
            sync.started += 1;
            sync.started
        };

        loop {
            if volume.has_failed() {
                return Err(StoreError::VolumeFailed { volume: volume.id });
            }

            let mut sync = hold(&volume.sync);
            if sync.completed >= wanted {
                return Ok(());
            }
            if sync.is_running {
                drop(sync);
                self.io.pause();
                continue;
            }
            sync.is_running = true;
            let covers = sync.started;
            drop(sync);

            let outcome = self.run_sync(volume);
            let mut sync = hold(&volume.sync);
            sync.is_running = false;
            match outcome {
                Ok(()) => sync.completed = covers,
                Err(source) => {
                    drop(sync);
                    volume.fail();
                    return Err(StoreError::Io {
                        action: "syncing a volume",
                        source,
                    });
                },
            }
        }
    }

    /// Sync each open segment: records first, then the rows that list them
    pub fn run_sync(&self, volume: &Volume<Backend>) -> Result<(), Error> {
        let mut slot = 0;
        while let Some(found) = next_open(volume, slot) {
            let (next, segment_slot, data, rows) = found;
            slot = next;

            data.sync()?;
            self.write_out_turn(segment_slot, &rows, true)?;
        }
        Ok(())
    }

    /// Send a slot's waiting rows to the kernel
    pub fn write_out(&self, slot: OpenSlot) -> Result<(), Error> {
        let Some(rows) = self.rows_file(slot) else {
            return Ok(());
        };
        self.write_out_turn(slot, &rows, false)
    }

    /// Find the rows file of an open slot
    pub fn rows_file(&self, slot: OpenSlot) -> Option<Backend::File> {
        for volume in &self.volumes {
            let claims = hold(&volume.claims);
            for segment in &claims.open {
                if segment.slot == slot {
                    return Some(segment.rows.clone());
                }
            }
        }
        None
    }

    /// Take the slot's writing turn, send its rows, and sync them when asked
    pub fn write_out_turn(&self, slot: OpenSlot, rows: &Backend::File, should_sync: bool) -> Result<(), Error> {
        let index = slot.0 as usize;
        let mut spare = loop {
            let mut queue = hold(&self.queues[index]);
            if !queue.is_writing {
                queue.is_writing = true;
                let mut spare_slot = hold(&self.spare_groups[index]);
                let Some(mut spare) = spare_slot.take() else {
                    queue.is_writing = false;
                    return Err(Error::other("rows queue lost its spare group"));
                };
                mem::swap(&mut queue.filling, &mut spare);
                break spare;
            }
            drop(queue);
            self.io.pause();
        };

        let (rows_end, synced_through) = {
            let queue = hold(&self.queues[index]);
            (queue.rows_end, queue.synced_through)
        };

        let mut outcome = Ok(());
        let mut new_end = rows_end;
        if !spare.is_empty() {
            let bytes = spare.finish(synced_through, self.record_key);
            let parts = [IoSlice::new(bytes)];
            outcome = rows.write_at(rows_end, &parts);
            new_end = FileOffset(rows_end.0 + bytes.len() as u64);
        }
        if outcome.is_ok() && should_sync {
            outcome = rows.sync();
        }

        spare.clear();
        *hold(&self.spare_groups[index]) = Some(spare);
        let mut queue = hold(&self.queues[index]);
        queue.is_writing = false;
        if outcome.is_ok() {
            queue.rows_end = new_end;
            if should_sync {
                queue.synced_through = new_end;
            }
        }
        outcome
    }
}

/// Find the first open segment past a slot, with handles copied out of the lock
pub fn next_open<Backend: Io>(
    volume: &Volume<Backend>,
    after: usize,
) -> Option<(usize, OpenSlot, Backend::File, Backend::File)> {
    let claims = hold(&volume.claims);
    let segment = claims.open.get(after)?;
    Some((after + 1, segment.slot, segment.data.clone(), segment.rows.clone()))
}

/// Pick the taking segment with the fewest writers, unless the volume should open another
pub fn pick_segment<Backend: Io>(
    claims: &ClaimState<Backend::File>,
    volume: &Volume<Backend>,
    request: &ClaimRequest,
) -> Option<usize> {
    let mut best: Option<usize> = None;
    let mut taking = 0;
    for (index, segment) in claims.open.iter().enumerate() {
        if segment.phase != SegmentPhase::Taking {
            continue;
        }
        let fits = segment.write_end.0 + request.bytes.0 <= volume.segment_bytes.0.max(request.bytes.0);
        if !fits {
            continue;
        }
        taking += 1;
        let is_better = match best {
            Some(current) => segment.in_flight < claims.open[current].in_flight,
            None => true,
        };
        if is_better {
            best = Some(index);
        }
    }

    let most_open = volume.roots.len() * OPEN_PER_ROOT;
    if let Some(index) = best
        && claims.open[index].in_flight >= WRITES_PER_OPEN
        && taking < most_open
    {
        return None;
    }
    best
}

/// Refuse a value the volume can't hold
pub fn check_value_size<Backend: Io>(
    volume: &Volume<Backend>,
    value: &[u8],
    codec: Codec,
    lz4_limit: ByteCount,
) -> Result<(), StoreError> {
    let length = ByteCount(value.len() as u64);
    if length > volume.segment_bytes {
        return Err(StoreError::ValueTooLarge {
            length,
            limit: volume.segment_bytes,
        });
    }
    let _codec_cap_is_on_stored_bytes = (codec, lz4_limit);
    Ok(())
}

/// Refuse a write to a failed volume
pub fn check_writable<Backend: Io>(volume: &Volume<Backend>) -> Result<(), StoreError> {
    if volume.has_failed() {
        return Err(StoreError::VolumeFailed { volume: volume.id });
    }
    Ok(())
}

