//! Puts, deletes and syncs

use std::io::{Error, ErrorKind, IoSlice};
use std::mem;
use std::sync::atomic::Ordering;

use reel_api::column_id::ColumnId;
use reel_api::store_error::StoreError;
use reel_api::units::{ByteCount, FileOffset, Nanoseconds, RootId, RowCount, WritePlace};

use crate::config::SyncPolicy;
use crate::engine::claim::Claim;
use crate::engine::engine_sizing::OPEN_PER_ROOT;
use crate::engine::engine_state::{EngineState, SlotData, SlotPin};
use crate::engine::open_segment::{
    MOST_WINDOW_RECORD_BYTES, OpenSegment, RECORD_WINDOW_BYTES, RecordWindow, SegmentPhase, data_path, rows_path,
};
use crate::engine::volume::{ClaimState, Volume};
use crate::guard::hold;
use crate::index::open_slots::OpenSlot;
use crate::index::write_row::WriteRow;
use crate::io::{Io, IoFile, IoTask};
use crate::record::codec::{CHUNK_BYTES, Codec};
use crate::record::record_check::RecordIdentity;
use crate::record::record_encoder::{PlainRecord, RecordEncoder, RecordShape};
use crate::record::record_head::plain_length;
use crate::row_kind::RowKind;
use crate::rows::row_group::{RowEntry, row_size};
use crate::segment::range_covers::{Cover, MOST_RANGE_COVERS};

/// In-flight writes an open segment takes before the volume opens another
pub const WRITES_PER_OPEN: u32 = 2;
/// How long a writer waits for a seal to free the write index before it reports the failure
pub const INDEX_WAIT: Nanoseconds = Nanoseconds(60_000_000_000);

/// What a claim asks for
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClaimRequest {
    pub bytes: ByteCount,
    pub rows: RowCount,
    pub is_alone: bool,
    pub has_records: bool,
    pub covers: usize,
}

/// A value on its way to a record
#[derive(Clone, Copy, Debug)]
pub struct ValueWrite<'bytes> {
    pub column: ColumnId,
    pub key: &'bytes [u8],
    pub value: &'bytes [u8],
    pub codec: Codec,
}

/// How an attempt to land a row ended
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LandOutcome {
    /// The row is in the index and the queue
    Landed,
    /// The index has no room, so nothing changed
    IndexFull,
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
        check_value_size(volume, value)?;
        check_writable(volume)?;

        let write = ValueWrite {
            column,
            key,
            value,
            codec: info.codec,
        };
        if info.codec == Codec::None && value.len() <= CHUNK_BYTES {
            return self.write_plain(volume, &write);
        }

        self.encoders.with(|encoder| self.write_encoded(volume, &write, encoder))
    }

    /// Land a small record in its slot's window, so the put makes no write call: the record reaches the kernel with
    /// the rows that list it, and a put a sync covers returns after that sync
    pub fn land_held(
        &self,
        volume: &Volume<Backend>,
        write: &ValueWrite,
        record: &PlainRecord,
        length: ByteCount,
    ) -> Result<(), StoreError> {
        let request = ClaimRequest {
            bytes: ByteCount(0),
            rows: RowCount(1),
            is_alone: false,
            has_records: true,
            covers: 0,
        };
        let size = row_size(write.key.len());
        loop {
            let claim = self.claim(volume, &request)?;
            let (slot, place) = (claim.slot, claim.first_place());
            let head = record.head(self.record_key, place);
            let Some(offset) = self.try_window(slot, [&head, write.value], size) else {
                // a writer that sleeps or writes with a ticket stops the mark for every writer behind it
                drop(claim);
                if self.refill_window(volume, slot, length.0 as usize, size)? {
                    continue;
                }
                return self.land_record(volume, write, length, |claim| {
                    let head = record.head(self.record_key, claim.first_place());
                    claim.data.write_at(claim.offset, &[IoSlice::new(&head), IoSlice::new(write.value)])
                });
            };

            let row = WriteRow {
                place,
                offset,
                length,
                kind: RowKind::Value,
                slot,
                segment: claim.segment,
                has_newer_sealed: false,
            };
            let queue = &self.queues[slot.0 as usize];
            if !self.index.insert(write.column, write.key, &row) {
                hold(queue).reserved -= size;
                drop(claim);
                self.wait_for_index(write.column, write.key, slot)?;
                continue;
            }

            let entry = RowEntry {
                column: write.column,
                key: write.key,
                place,
                offset,
                length,
                kind: RowKind::Value,
            };
            {
                let mut guard = hold(queue);
                guard.reserved -= size;
                guard.filling.push(&entry);
            }
            self.wake_row_job();
            claim.land();
            return self.after_write(volume, length);
        }
    }

    /// Copy a record into a slot's window and hold room for its row, or return nothing when either has no room
    pub fn try_window(&self, slot: OpenSlot, parts: [&[u8]; 2], size: usize) -> Option<FileOffset> {
        let index = slot.0 as usize;
        let length = parts[0].len() + parts[1].len();
        let mut guard = hold(&self.queues[index]);
        let window = &self.windows[index][guard.live_window];
        if !guard.filling.has_room(guard.reserved + size) || window.used() + length > guard.window_room {
            return None;
        }
        guard.reserved += size;
        Some(window.push(parts))
    }

    /// Make room in a slot's window and group, with no claim held, and return false when the segment has too little
    /// left for a window
    pub fn refill_window(
        &self,
        volume: &Volume<Backend>,
        slot: OpenSlot,
        length: usize,
        size: usize,
    ) -> Result<bool, StoreError> {
        let queue = &self.queues[slot.0 as usize];
        let mut guard = hold(queue);
        let has_row_room = guard.filling.has_room(guard.reserved + size);
        let window = &self.windows[slot.0 as usize][guard.live_window];
        let has_record_room = window.used() + length <= guard.window_room;
        if has_row_room && has_record_room {
            return Ok(true);
        }

        let needs_window = !has_record_room && !guard.is_refilling;
        if needs_window {
            // nothing goes in the rest of this range, since the range after it replaces it
            guard.is_refilling = true;
            guard.window_room = window.used();
        }
        drop(guard);
        if !needs_window && has_row_room {
            self.io.pause();
            return Ok(true);
        }

        let sent = self.write_out(slot).map_err(|source| StoreError::Io {
            action: "writing rows",
            source,
        });
        let has_window = !needs_window || sent.is_ok() && self.claim_window(volume, slot, length);
        if needs_window {
            hold(queue).is_refilling = false;
        }
        sent?;
        Ok(has_window)
    }

    /// Claim the next range of a segment for its window, and return false when too little of the segment is left
    pub fn claim_window(&self, volume: &Volume<Backend>, slot: OpenSlot, length: usize) -> bool {
        let mut claims = hold(&volume.claims);
        let Some(segment) = claims.open.iter_mut().find(|segment| segment.slot == slot) else {
            // the segment went while no claim was held, and the next claim finds another
            return true;
        };
        if segment.phase != SegmentPhase::Taking {
            return true;
        }
        // a window that still holds records cannot start over, and the refill that follows sends them
        {
            let guard = hold(&self.queues[slot.0 as usize]);
            if !self.windows[slot.0 as usize][guard.live_window].is_empty() {
                return true;
            }
        }
        let left = volume.segment_bytes.0.saturating_sub(segment.write_end.0) as usize;
        let room = left.min(RECORD_WINDOW_BYTES);
        if room < length {
            return false;
        }
        let start = segment.write_end;
        segment.write_end = FileOffset(start.0 + room as u64);
        drop(claims);
        let mut guard = hold(&self.queues[slot.0 as usize]);
        self.windows[slot.0 as usize][guard.live_window].reset(start);
        guard.window_room = room;
        true
    }

    /// Write a one-chunk value as it stands, with its head on the stack, so no writer waits for a shared encoder
    pub fn write_plain(&self, volume: &Volume<Backend>, write: &ValueWrite) -> Result<(), StoreError> {
        let identity = RecordIdentity {
            record_key: self.record_key,
            column: write.column,
            key: write.key,
            kind: RowKind::Value,
            place: WritePlace(0),
        };
        let record = PlainRecord::new(&identity, write.value);
        let length = plain_length(ByteCount(write.value.len() as u64));
        if length.0 as usize <= MOST_WINDOW_RECORD_BYTES {
            return self.land_held(volume, write, &record, length);
        }
        self.land_record(volume, write, length, |claim| {
            let head = record.head(self.record_key, claim.first_place());
            claim.data.write_at(claim.offset, &[IoSlice::new(&head), IoSlice::new(write.value)])
        })
    }

    /// Lay a value out and write it, starting over when the index has no room for its row
    pub fn write_encoded(
        &self,
        volume: &Volume<Backend>,
        write: &ValueWrite,
        encoder: &mut RecordEncoder,
    ) -> Result<(), StoreError> {
        let identity = RecordIdentity {
            record_key: self.record_key,
            column: write.column,
            key: write.key,
            kind: RowKind::Value,
            place: WritePlace(0),
        };
        let shape = self.encode_value(encoder, &identity, write.codec, write.value)?;

        let length = shape.total_length();
        let landed = self.land_record(volume, write, length, |claim| {
            encoder.set_place(self.record_key, claim.first_place());
            encoder.write_to(&claim.data, claim.offset, &shape, write.value)
        });
        encoder.give_shares(&self.stored_shares);
        landed
    }

    /// Lay a value out with the shares it may need, which the caller gives back once the record is written
    pub fn encode_value(
        &self,
        encoder: &mut RecordEncoder,
        identity: &RecordIdentity,
        codec: Codec,
        value: &[u8],
    ) -> Result<RecordShape, StoreError> {
        let shape = encoder
            .take_shares(&self.stored_shares, codec, value)
            .then(|| encoder.encode(identity, codec, value))
            .flatten();
        shape.ok_or_else(|| {
            encoder.give_shares(&self.stored_shares);
            StoreError::ValueTooLarge {
                length: ByteCount(value.len() as u64),
                limit: self.sizing.lz4_stored_limit,
            }
        })
    }

    /// Claim room for a record, write it, and land its row, starting over when the index has no room
    pub fn land_record(
        &self,
        volume: &Volume<Backend>,
        write: &ValueWrite,
        length: ByteCount,
        mut write_record: impl FnMut(&Claim<'_, Backend>) -> std::io::Result<()>,
    ) -> Result<(), StoreError> {
        let request = ClaimRequest {
            bytes: length,
            rows: RowCount(1),
            is_alone: false,
            has_records: true,
            covers: 0,
        };
        loop {
            let claim = self.claim(volume, &request)?;
            if let Err(source) = write_record(&claim) {
                return Err(store_error(volume, "writing a record", source));
            }

            let row = WriteRow {
                place: claim.first_place(),
                offset: claim.offset,
                length,
                kind: RowKind::Value,
                slot: claim.slot,
                segment: claim.segment,
                has_newer_sealed: false,
            };
            let slot = claim.slot;
            match self.land_row(write.column, write.key, &row)? {
                LandOutcome::Landed => {
                    claim.land();
                    return self.after_write(volume, length);
                },
                LandOutcome::IndexFull => {
                    // the claim ends here, so the seal that makes room doesn't wait for this writer
                    drop(claim);
                    self.wait_for_index(write.column, write.key, slot)?;
                },
            }
        }
    }

    /// Write a grave, unless the key's newest row in the index already is one
    pub fn write_grave(&self, column: ColumnId, key: &[u8]) -> Result<(), StoreError> {
        let info = self.column(column)?;
        self.check_key(column, key)?;
        let volume = &self.volumes[info.volume.0 as usize];
        check_writable(volume)?;
        if self.index.is_newest_grave(column, key, self.read_point(), volume.view()) {
            return Ok(());
        }

        let request = ClaimRequest {
            bytes: ByteCount(0),
            rows: RowCount(1),
            is_alone: false,
            has_records: false,
            covers: 0,
        };
        loop {
            let claim = self.claim(volume, &request)?;
            let row = WriteRow {
                place: claim.first_place(),
                offset: claim.offset,
                length: ByteCount(0),
                kind: RowKind::Grave,
                slot: claim.slot,
                segment: claim.segment,
                has_newer_sealed: false,
            };
            let slot = claim.slot;
            match self.land_row(column, key, &row)? {
                LandOutcome::Landed => {
                    claim.land();
                    self.deletes.fetch_add(1, Ordering::Relaxed);
                    return self.after_write(volume, ByteCount(0));
                },
                LandOutcome::IndexFull => {
                    drop(claim);
                    self.wait_for_index(column, key, slot)?;
                },
            }
        }
    }

    /// Remove every key in [start, end) of a column written before this call, as one record
    pub fn write_range_delete(&self, column: ColumnId, start: &[u8], end: &[u8]) -> Result<(), StoreError> {
        let info = self.column(column)?;
        self.check_key(column, start)?;
        self.check_key(column, end)?;
        let volume = &self.volumes[info.volume.0 as usize];
        check_writable(volume)?;
        if start >= end {
            return Ok(());
        }
        if !volume.covers.hold(1) {
            return Err(StoreError::RangeDeletesFull { volume: volume.id });
        }

        let landed = self.land_range_delete(volume, column, start, end);
        if landed.is_err() {
            volume.covers.release(1);
        }
        let length = landed?;
        self.after_write(volume, length)
    }

    /// Write the record of a range delete and land its row and cover, a hold having made room for the cover, and
    /// return the length of the record
    pub fn land_range_delete(
        &self,
        volume: &Volume<Backend>,
        column: ColumnId,
        start: &[u8],
        end: &[u8],
    ) -> Result<ByteCount, StoreError> {
        let identity = RecordIdentity {
            record_key: self.record_key,
            column,
            key: start,
            kind: RowKind::RangeDelete,
            place: WritePlace(0),
        };
        let record = PlainRecord::new(&identity, end);
        let length = plain_length(ByteCount(end.len() as u64));
        let request = ClaimRequest {
            bytes: length,
            rows: RowCount(1),
            is_alone: false,
            has_records: true,
            covers: 1,
        };
        loop {
            let claim = self.claim(volume, &request)?;
            let head = record.head(self.record_key, claim.first_place());
            if let Err(source) = claim.data.write_at(claim.offset, &[IoSlice::new(&head), IoSlice::new(end)]) {
                return Err(store_error(volume, "writing a record", source));
            }

            let row = WriteRow {
                place: claim.first_place(),
                offset: claim.offset,
                length,
                kind: RowKind::RangeDelete,
                slot: claim.slot,
                segment: claim.segment,
                has_newer_sealed: false,
            };
            let slot = claim.slot;
            match self.land_row(column, start, &row)? {
                LandOutcome::Landed => {
                    let cover = Cover {
                        column,
                        start,
                        end,
                        place: row.place,
                        segment: row.segment,
                    };
                    volume.covers.add_held(&cover);
                    claim.land();
                    // older covers leave only now, since a reader below this place still needs them
                    volume.covers.drop_inside(&cover);
                    return Ok(length);
                },
                LandOutcome::IndexFull => {
                    drop(claim);
                    self.wait_for_index(column, start, slot)?;
                },
            }
        }
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

    /// Put a row in the write index and the rows queue, or in neither
    pub fn land_row(&self, column: ColumnId, key: &[u8], row: &WriteRow) -> Result<LandOutcome, StoreError> {
        let size = row_size(key.len());
        self.reserve_row(row.slot, size)?;

        let queue = &self.queues[row.slot.0 as usize];
        if !self.index.insert(column, key, row) {
            hold(queue).reserved -= size;
            return Ok(LandOutcome::IndexFull);
        }

        let entry = RowEntry {
            column,
            key,
            place: row.place,
            offset: row.offset,
            length: row.length,
            kind: row.kind,
        };
        {
            let mut guard = hold(queue);
            guard.reserved -= size;
            // the reservation holds this room, whichever group is filling by now
            guard.filling.push(&entry);
        }
        self.wake_row_job();
        Ok(LandOutcome::Landed)
    }

    /// Hold room for a row in the group filling, sending the group out when it's full
    pub fn reserve_row(&self, slot: OpenSlot, size: usize) -> Result<(), StoreError> {
        let queue = &self.queues[slot.0 as usize];
        loop {
            {
                let mut guard = hold(queue);
                if guard.filling.has_room(guard.reserved + size) {
                    guard.reserved += size;
                    return Ok(());
                }
            }
            self.write_out(slot).map_err(|source| StoreError::Io {
                action: "writing rows",
                source,
            })?;
        }
    }

    /// Wake the row job, which only sleeps with no deadline when it found nothing to do
    pub fn wake_row_job(&self) {
        if self.row_job_idle.swap(false, Ordering::AcqRel)
            && let Some(task) = self.row_task.get()
        {
            task.wake();
        }
    }

    /// Start seals and wait for the write index to have room, or report that it never did
    pub fn wait_for_index(&self, column: ColumnId, key: &[u8], slot: OpenSlot) -> Result<(), StoreError> {
        let started = self.io.now();
        self.index_waits.fetch_add(1, Ordering::Relaxed);
        let deadline = self.io.now().0.saturating_add(INDEX_WAIT.0);
        loop {
            for volume in &self.volumes {
                self.seal_fullest(volume);
            }
            if self.index.has_room(column, key, slot) {
                self.longest_index_wait.fetch_max(self.io.now().0.saturating_sub(started.0), Ordering::Relaxed);
                return Ok(());
            }
            if self.io.now().0 > deadline {
                return Err(StoreError::Io {
                    action: "waiting for the write index",
                    source: Error::other("no seal freed room in the write index"),
                });
            }
            self.io.pause();
        }
    }

    /// Claim bytes and places together, opening a segment first when none has room
    pub fn claim<'state>(
        &'state self,
        volume: &'state Volume<Backend>,
        request: &ClaimRequest,
    ) -> Result<Claim<'state, Backend>, StoreError> {
        let deadline = self.io.now().0.saturating_add(INDEX_WAIT.0);
        loop {
            // a failed volume frees no slot, so a wait for one would never end
            check_writable(volume)?;
            if self.io.now().0 > deadline {
                return Err(StoreError::Io {
                    action: "waiting for an open segment",
                    source: Error::other("no segment or slot freed up"),
                });
            }

            let mut claims = hold(&volume.claims);
            let pick = pick_segment(&claims, volume, request);
            if pick.wants_more || pick.best.is_none() {
                // a segment made ahead costs a claim nothing, and the job starts on the next
                if let Some(segment) = claims.prepared.take() {
                    volume.open_slot(segment.slot);
                    claims.open.push(segment);
                    self.ask_for_segment(&mut claims);
                    continue;
                }
                if pick.wants_more {
                    self.ask_for_segment(&mut claims);
                }
            }

            if let Some(index) = pick.best {
                let Some(ticket) = self.order.try_claim(request.rows) else {
                    // the ring frees when claims end, and ending one needs this lock, and so does a raise
                    drop(claims);
                    if self.order.is_past_limit(request.rows) {
                        let _is_held = self.reserve_ahead();
                    }
                    self.io.pause();
                    continue;
                };

                let segment = &mut claims.open[index];
                if request.is_alone {
                    segment.phase = SegmentPhase::Alone;
                }
                let offset = segment.write_end;
                segment.write_end = FileOffset(offset.0 + request.bytes.0);
                segment.row_count = RowCount(segment.row_count.0 + request.rows.0);
                segment.in_flight += 1;
                segment.cover_rows += request.covers;
                segment.lowest_place = segment.lowest_place.min(ticket.first);
                segment.highest_place = segment.highest_place.max(ticket.last);
                let number = segment.number;
                let data = segment.data.clone();
                let slot = segment.slot;
                self.slot_files[slot.0 as usize].pins.fetch_add(1, Ordering::AcqRel);
                self.mark_full_segments(&mut claims, volume);
                return Ok(Claim {
                    state: self,
                    volume,
                    slot,
                    segment: number,
                    data,
                    offset,
                    ticket,
                    is_landed: false,
                    is_alone: request.is_alone,
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

    /// Start a seal when the open segments together pass their limits
    pub fn mark_full_segments(&self, claims: &mut ClaimState<Backend::File>, volume: &Volume<Backend>) {
        let mut rows = 0;
        let mut bytes = 0;
        for segment in &claims.open {
            if segment.phase == SegmentPhase::Taking {
                rows += segment.row_count.0;
                bytes += segment.write_end.0;
            }
        }

        // a seal lists the covers of a segment in a fixed buffer, so a segment at the limit seals whatever its size
        for segment in &mut claims.open {
            if segment.phase == SegmentPhase::Taking && segment.cover_rows >= MOST_RANGE_COVERS {
                segment.phase = SegmentPhase::Sealing;
                if let Some(task) = self.seal_tasks[volume.id.0 as usize].get() {
                    task.wake();
                }
            }
        }

        // half full is time for the job to have the next one ready before this one seals
        let is_half_full = rows * 2 >= volume.segment_rows.0 || bytes * 2 >= volume.segment_bytes.0;
        if is_half_full && claims.prepared.is_none() {
            self.ask_for_segment(claims);
        }

        if rows >= volume.segment_rows.0 || bytes >= volume.segment_bytes.0 {
            self.mark_fullest(claims, volume);
        }
    }

    /// Ask the job for a segment made ahead, unless it has been asked
    pub fn ask_for_segment(&self, claims: &mut ClaimState<Backend::File>) {
        if !claims.wants_prepared {
            claims.wants_prepared = true;
            if let Some(task) = self.reserve_task.get() {
                task.wake();
            }
        }
    }

    /// Make the segments writers asked for, with the preallocation and directory sync no put should wait on. A full
    /// disk leaves none ready, and the writer that needs one hears it when it creates its own
    pub fn prepare_segments(&self) {
        for volume in &self.volumes {
            let root = {
                let mut claims = hold(&volume.claims);
                if !claims.wants_prepared || claims.prepared.is_some() || claims.is_creating || volume.has_failed() {
                    continue;
                }
                claims.wants_prepared = false;
                claims.is_creating = true;
                let root = claims.next_root;
                claims.next_root = (root + 1) % volume.roots.len();
                root
            };

            let request = ClaimRequest {
                bytes: ByteCount(0),
                rows: RowCount(0),
                is_alone: false,
                has_records: true,
                covers: 0,
            };
            let created = self.create_segment(volume, RootId(root as u8), &request);
            let mut claims = hold(&volume.claims);
            claims.is_creating = false;
            if let Ok(Some(segment)) = created {
                claims.prepared = Some(segment);
            }
        }
    }

    /// Remove a segment made ahead that never took a claim
    pub fn drop_prepared(&self, volume: &Volume<Backend>) -> Result<(), Error> {
        let Some(segment) = hold(&volume.claims).prepared.take() else {
            return Ok(());
        };
        let root = &volume.roots[segment.root.0 as usize];
        self.io.close_file(segment.data);
        self.io.close_file(segment.rows);
        self.slot_files[segment.slot.0 as usize].set(None);
        self.free_slot(segment.slot);
        self.io.remove_file(&data_path(root, segment.number))?;
        self.io.remove_file(&rows_path(root, segment.number))?;
        self.io.sync_directory(root)
    }

    /// Mark the fullest taking segment to seal
    pub fn mark_fullest(&self, claims: &mut ClaimState<Backend::File>, volume: &Volume<Backend>) {
        let mut fullest: Option<usize> = None;
        for (index, segment) in claims.open.iter().enumerate() {
            if segment.phase == SegmentPhase::Taking && segment.row_count.0 > 0 {
                let is_fuller = match fullest {
                    Some(best) => segment.write_end > claims.open[best].write_end,
                    None => true,
                };
                if is_fuller {
                    fullest = Some(index);
                }
            }
        }

        if let Some(index) = fullest {
            claims.open[index].phase = SegmentPhase::Sealing;
            if let Some(task) = self.seal_tasks[volume.id.0 as usize].get() {
                task.wake();
            }
        }
    }

    /// Mark every open segment with no rows to seal, which drops it
    pub fn seal_empty(&self, volume: &Volume<Backend>) {
        let mut claims = hold(&volume.claims);
        for segment in &mut claims.open {
            if segment.phase == SegmentPhase::Taking && segment.row_count.0 == 0 {
                segment.phase = SegmentPhase::Sealing;
            }
        }
    }

    /// Mark one open segment to seal
    pub fn seal_slot(&self, volume: &Volume<Backend>, slot: OpenSlot) {
        let mut claims = hold(&volume.claims);
        for segment in &mut claims.open {
            if segment.slot == slot {
                segment.phase = SegmentPhase::Sealing;
            }
        }
        if let Some(task) = self.seal_tasks[volume.id.0 as usize].get() {
            task.wake();
        }
    }

    /// Mark a volume's fullest taking segment to seal
    pub fn seal_fullest(&self, volume: &Volume<Backend>) {
        let mut claims = hold(&volume.claims);
        self.mark_fullest(&mut claims, volume);
    }

    /// Clear the windows of a slot for a new segment
    pub fn reset_windows(&self, slot: OpenSlot) {
        for window in &self.windows[slot.0 as usize] {
            window.reset(FileOffset(0));
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
        if request.has_records && free.0 < volume.reserve.0 + volume.segment_bytes.0 {
            return Err(StoreError::VolumeFull { volume: volume.id });
        }

        let Some(slot) = self.take_slot() else {
            return Ok(None);
        };
        match self.create_files(volume, root, slot, request) {
            Ok(segment) => Ok(Some(segment)),
            Err(error) => {
                self.free_slot(slot);
                Err(error)
            },
        }
    }

    /// Create the data and rows files, leaving neither behind on a failure
    pub fn create_files(
        &self,
        volume: &Volume<Backend>,
        root: RootId,
        slot: OpenSlot,
        request: &ClaimRequest,
    ) -> Result<OpenSegment<Backend::File>, StoreError> {
        let path = &volume.roots[root.0 as usize];
        let number = self.next_segment_number()?;
        let reserved = if request.has_records {
            ByteCount(volume.segment_bytes.0.max(request.bytes.0))
        } else {
            ByteCount(0)
        };

        let data_file = data_path(path, number);
        let data = self
            .io
            .create_file(&data_file, reserved)
            .map_err(|source| store_error(volume, "creating a segment", source))?;

        let rows_file = rows_path(path, number);
        let rows = match self.io.create_file(&rows_file, ByteCount(0)) {
            Ok(rows) => rows,
            Err(source) => {
                self.io.close_file(data);
                // the file is empty, and open drops an empty file it finds
                let _removed = self.io.remove_file(&data_file);
                return Err(store_error(volume, "creating a rows file", source));
            },
        };

        if let Err(source) = self.io.sync_directory(path) {
            self.io.close_file(data);
            self.io.close_file(rows);
            let _removed = self.io.remove_file(&data_file);
            let _removed = self.io.remove_file(&rows_file);
            return Err(store_error(volume, "syncing a root", source));
        }

        hold(&self.queues[slot.0 as usize]).reset(FileOffset(0), FileOffset(0));
        self.reset_windows(slot);
        self.slot_files[slot.0 as usize].set(Some(SlotData {
            segment: number,
            file: data.clone(),
        }));
        Ok(OpenSegment {
            number,
            slot,
            root,
            data,
            rows,
            phase: SegmentPhase::Taking,
            write_end: FileOffset(0),
            row_count: RowCount(0),
            in_flight: 0,
            cover_rows: 0,
            lowest_place: WritePlace(u64::MAX),
            highest_place: WritePlace(0),
        })
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

    /// Sync each open segment through its writing turn, walking by slot since seals remove segments
    pub fn run_sync(&self, volume: &Volume<Backend>) -> Result<(), Error> {
        let mut after: Option<OpenSlot> = None;
        while let Some((slot, pin, rows)) = self.next_open(volume, after) {
            after = Some(slot);
            self.write_out_turn(slot, &rows, Some(&pin.file))?;
        }
        Ok(())
    }

    /// Pin the open segment with the lowest slot past another
    pub fn next_open(
        &self,
        volume: &Volume<Backend>,
        after: Option<OpenSlot>,
    ) -> Option<(OpenSlot, SlotPin<'_, Backend::File>, Backend::File)> {
        let claims = hold(&volume.claims);
        let mut best: Option<&OpenSegment<Backend::File>> = None;
        for segment in &claims.open {
            let is_past = after.is_none_or(|slot| segment.slot > slot);
            if is_past && best.is_none_or(|current| segment.slot < current.slot) {
                best = Some(segment);
            }
        }

        let segment = best?;
        let pin = self.slot_files[segment.slot.0 as usize].pin(segment.number)?;
        Some((segment.slot, pin, segment.rows.clone()))
    }

    /// Send a slot's waiting rows to the kernel
    pub fn write_out(&self, slot: OpenSlot) -> Result<(), Error> {
        let Some((pin, rows)) = self.pin_rows(slot) else {
            return Ok(());
        };
        let outcome = self.write_out_turn(slot, &rows, None);
        drop(pin);
        outcome
    }

    /// Send a slot's waiting rows unless its turn is out, so a long group holds back no other slot's rows
    pub fn write_out_if_free(&self, slot: OpenSlot) -> Result<(), Error> {
        let Some((pin, rows)) = self.pin_rows(slot) else {
            return Ok(());
        };
        if !self.try_writing_turn(slot) {
            return Ok(());
        }
        let outcome = self.send_in_turn(slot, &rows, None);
        drop(pin);
        outcome
    }

    /// Pin an open slot and find its rows file, which a seal closes only once the pins are gone
    pub fn pin_rows(&self, slot: OpenSlot) -> Option<(SlotPin<'_, Backend::File>, Backend::File)> {
        for volume in &self.volumes {
            let claims = hold(&volume.claims);
            for segment in &claims.open {
                if segment.slot == slot {
                    let pin = self.slot_files[slot.0 as usize].pin(segment.number)?;
                    return Some((pin, segment.rows.clone()));
                }
            }
        }
        None
    }

    /// Take the slot's writing turn if it's free
    pub fn try_writing_turn(&self, slot: OpenSlot) -> bool {
        let mut queue = hold(&self.queues[slot.0 as usize]);
        let is_free = !queue.is_writing;
        queue.is_writing = true;
        is_free
    }

    /// Wait for the slot's writing turn, which keeps its groups in order
    pub fn take_writing_turn(&self, slot: OpenSlot) {
        while !self.try_writing_turn(slot) {
            self.io.pause();
        }
    }

    /// Take the slot's writing turn and send its rows, in a sync when given the data file
    pub fn write_out_turn(&self, slot: OpenSlot, rows: &Backend::File, data: Option<&Backend::File>) -> Result<(), Error> {
        self.take_writing_turn(slot);
        self.send_in_turn(slot, rows, data)
    }

    /// Send a slot's rows with its turn held, and give the turn back
    pub fn send_in_turn(&self, slot: OpenSlot, rows: &Backend::File, data: Option<&Backend::File>) -> Result<(), Error> {
        let index = slot.0 as usize;

        let mut outcome = self.send_group(slot, rows, data);
        if outcome.is_ok() && self.has_left_over_rows(slot) {
            outcome = self.send_group(slot, rows, data);
        }
        hold(&self.queues[index]).is_writing = false;
        outcome
    }

    /// Check rows are waiting beyond the group just sent
    pub fn has_left_over_rows(&self, slot: OpenSlot) -> bool {
        !hold(&self.queues[slot.0 as usize]).filling.is_empty()
    }

    /// Send one group: fix its rows first, then sync their records, then write and sync the rows
    pub fn send_group(&self, slot: OpenSlot, rows: &Backend::File, data: Option<&Backend::File>) -> Result<(), Error> {
        let index = slot.0 as usize;
        let mut group = {
            let mut spare_slot = hold(&self.spare_groups[index]);
            let Some(group) = spare_slot.take() else {
                return Err(Error::other("rows queue lost its spare group"));
            };
            group
        };

        let (rows_end, synced_through, mut marked_end) = {
            let mut queue = hold(&self.queues[index]);
            // a group a failed write left behind goes first, and whole
            if group.is_empty() {
                mem::swap(&mut queue.filling, &mut group);
            }
            (queue.rows_end, queue.synced_through, queue.marked_end)
        };

        // records go first, so the rows that list them never reach the kernel alone
        let mut outcome = self.send_windows(index);
        if outcome.is_ok()
            && let Some(data) = data
        {
            // only records of rows already fixed in this group count as covered
            outcome = data.sync();
        }

        let mut new_end = rows_end;
        if outcome.is_ok() && !group.is_empty() {
            let bytes = group.finish(synced_through, self.record_key);
            let parts = [IoSlice::new(bytes)];
            outcome = rows.write_at(rows_end, &parts);
            new_end = FileOffset(rows_end.0 + bytes.len() as u64);
        }
        if outcome.is_ok() && data.is_some() {
            outcome = rows.sync();
        }

        let mut covered = synced_through;
        if outcome.is_ok() {
            group.clear();
            if data.is_some() {
                covered = new_end;
                // only a group written after the sync can say it finished, and with nothing new there's nothing to say
                if new_end > marked_end {
                    let bytes = group.finish(covered, self.record_key);
                    let parts = [IoSlice::new(bytes)];
                    let mark_end = FileOffset(new_end.0 + bytes.len() as u64);
                    // the sync is done whatever this does: a lost mark costs the next open a check of these records
                    if rows.write_at(new_end, &parts).is_ok() {
                        new_end = mark_end;
                        marked_end = mark_end;
                    }
                    group.clear();
                }
            }
            let mut queue = hold(&self.queues[index]);
            queue.rows_end = new_end;
            queue.synced_through = covered;
            queue.marked_end = marked_end;
        }
        *hold(&self.spare_groups[index]) = Some(group);
        outcome
    }

    /// Send the records of a slot to the kernel: a window a failed write left first, then the live one, whose rest of the
    /// range goes on in the other window
    pub fn send_windows(&self, index: usize) -> Result<(), Error> {
        let windows = &self.windows[index];
        loop {
            let (window, was_kept) = {
                let mut queue = hold(&self.queues[index]);
                let live = queue.live_window;
                if queue.has_kept_window {
                    (&windows[1 - live], true)
                } else {
                    if windows[live].is_empty() {
                        return Ok(());
                    }
                    windows[1 - live].reset(windows[live].end());
                    queue.window_room -= windows[live].used();
                    queue.live_window = 1 - live;
                    queue.has_kept_window = true;
                    (&windows[live], false)
                }
            };

            self.send_window(index, window)?;
            hold(&self.queues[index]).has_kept_window = false;
            // reads go to the file from here, before any sync, so a sync never holds one up
            window.reset(FileOffset(0));
            if !was_kept {
                return Ok(());
            }
        }
    }

    /// Send a window's records to the kernel
    pub fn send_window(&self, index: usize, window: &RecordWindow) -> Result<(), Error> {
        let Some(file) = self.slot_files[index].file() else {
            return Err(Error::other("a window has no data file"));
        };
        let mut copy = hold(&self.window_copies[index]);
        let length = window.copy_out(&mut copy);
        file.write_at(window.start(), &[IoSlice::new(&copy[..length])])
    }

    /// Check a slot has rows that haven't reached the kernel
    pub fn has_waiting_rows(&self, slot: OpenSlot) -> bool {
        let index = slot.0 as usize;
        let queue = hold(&self.queues[index]);
        let has_records = queue.has_kept_window || !self.windows[index][queue.live_window].is_empty();
        if !queue.filling.is_empty() || has_records {
            return true;
        }
        drop(queue);
        match hold(&self.spare_groups[index]).as_ref() {
            Some(group) => !group.is_empty(),
            None => true,
        }
    }
}

/// A claim's choice of segment
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SegmentPick {
    pub best: Option<usize>,
    pub wants_more: bool,
}

/// Pick the taking segment with the fewest writers, and say when the volume should open another
pub fn pick_segment<Backend: Io>(
    claims: &ClaimState<Backend::File>,
    volume: &Volume<Backend>,
    request: &ClaimRequest,
) -> SegmentPick {
    let mut best: Option<usize> = None;
    let mut taking = 0;
    for (index, segment) in claims.open.iter().enumerate() {
        if segment.phase != SegmentPhase::Taking {
            continue;
        }
        let has_bytes = segment.write_end.0 + request.bytes.0 <= volume.segment_bytes.0.max(request.bytes.0);
        let has_rows = segment.row_count.0 + request.rows.0 <= volume.segment_rows.0.max(request.rows.0);
        // a seal lists a segment's covers in a buffer sized for this many
        let has_cover_room = segment.cover_rows + request.covers <= MOST_RANGE_COVERS;
        let fits = has_bytes && has_rows && has_cover_room;
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
    let wants_more = best.is_some_and(|index| claims.open[index].in_flight >= WRITES_PER_OPEN) && taking < most_open;
    SegmentPick { best, wants_more }
}

/// Refuse a value larger than a segment of its volume
pub fn check_value_size<Backend: Io>(volume: &Volume<Backend>, value: &[u8]) -> Result<(), StoreError> {
    let length = ByteCount(value.len() as u64);
    if length > volume.segment_bytes {
        return Err(StoreError::ValueTooLarge {
            length,
            limit: volume.segment_bytes,
        });
    }
    Ok(())
}

/// Refuse a write to a failed volume
pub fn check_writable<Backend: Io>(volume: &Volume<Backend>) -> Result<(), StoreError> {
    if volume.has_failed() {
        return Err(StoreError::VolumeFailed { volume: volume.id });
    }
    Ok(())
}
