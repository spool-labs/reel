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
use crate::engine::open_segment::{OpenSegment, SegmentPhase, data_path, rows_path};
use crate::engine::volume::{ClaimState, Volume};
use crate::guard::hold;
use crate::index::open_slots::OpenSlot;
use crate::index::write_row::WriteRow;
use crate::io::{Io, IoFile, IoTask};
use crate::record::codec::{CHUNK_BYTES, Codec};
use crate::record::record_check::RecordIdentity;
use crate::record::record_encoder::RecordEncoder;
use crate::row_kind::RowKind;
use crate::rows::row_group::{RowEntry, row_size};

/// In-flight writes an open segment takes before the volume opens another
pub const WRITES_PER_OPEN: u32 = 2;
/// How long a writer waits for a seal to free the write index before it reports the failure
pub const INDEX_WAIT: Nanoseconds = Nanoseconds(60_000_000_000);

/// What a claim asks for
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClaimRequest {
    pub bytes: ByteCount,
    pub rows: RowCount,
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
        let is_large = info.codec == Codec::Lz4 && value.len() > CHUNK_BYTES;
        let encoders = if is_large { &self.large_encoders } else { &self.encoders };
        encoders.with(|encoder| self.write_encoded(volume, &write, encoder))
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
        };
        let Some(shape) = encoder.encode(&identity, write.codec, write.value) else {
            return Err(StoreError::ValueTooLarge {
                length: ByteCount(write.value.len() as u64),
                limit: self.sizing.lz4_stored_limit,
            });
        };

        let length = shape.total_length();
        let request = ClaimRequest {
            bytes: length,
            rows: RowCount(1),
        };
        loop {
            let claim = self.claim(volume, &request)?;
            let parts = encoder.parts(&shape, write.value);
            if let Err(source) = claim.data.write_at(claim.offset, &parts) {
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
        let deadline = self.io.now().0.saturating_add(INDEX_WAIT.0);
        loop {
            for volume in &self.volumes {
                self.seal_fullest(volume);
            }
            if self.index.has_room(column, key, slot) {
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
            if let Some(index) = pick_segment(&claims, volume, request) {
                let Some(ticket) = self.order.try_claim(request.rows) else {
                    // the ring frees when claims end, and ending one needs this lock, and so does a raise
                    drop(claims);
                    self.raise_place_limit(request.rows)?;
                    self.io.pause();
                    continue;
                };

                let segment = &mut claims.open[index];
                let offset = segment.write_end;
                segment.write_end = FileOffset(offset.0 + request.bytes.0);
                segment.row_count = RowCount(segment.row_count.0 + request.rows.0);
                segment.in_flight += 1;
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

        if rows >= volume.segment_rows.0 || bytes >= volume.segment_bytes.0 {
            self.mark_fullest(claims, volume);
        }
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
        let reserved = if request.bytes.0 > 0 {
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

    /// Take the slot's writing turn and send its rows, in a sync when given the data file
    pub fn write_out_turn(&self, slot: OpenSlot, rows: &Backend::File, data: Option<&Backend::File>) -> Result<(), Error> {
        let index = slot.0 as usize;
        loop {
            {
                let mut queue = hold(&self.queues[index]);
                if !queue.is_writing {
                    queue.is_writing = true;
                    break;
                }
            }
            self.io.pause();
        }

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

        let (rows_end, synced_through) = {
            let mut queue = hold(&self.queues[index]);
            // a group a failed write left behind goes first, and whole
            if group.is_empty() {
                mem::swap(&mut queue.filling, &mut group);
            }
            (queue.rows_end, queue.synced_through)
        };

        let mut outcome = Ok(());
        if let Some(data) = data {
            // only records of rows already fixed in this group count as covered
            outcome = data.sync();
        }

        let mut new_end = rows_end;
        // a sync with no new rows still writes an empty group, or open would read what it covered as unsynced
        let has_uncovered_rows = data.is_some() && synced_through < rows_end;
        if outcome.is_ok() && (!group.is_empty() || has_uncovered_rows) {
            // a sync turn synced this group's records before writing it, so the group covers itself
            let through = match data {
                Some(_data) => FileOffset(rows_end.0 + group.total_length().0),
                None => synced_through,
            };
            let bytes = group.finish(through, self.record_key);
            let parts = [IoSlice::new(bytes)];
            outcome = rows.write_at(rows_end, &parts);
            new_end = FileOffset(rows_end.0 + bytes.len() as u64);
        }
        if outcome.is_ok() && data.is_some() {
            outcome = rows.sync();
        }

        if outcome.is_ok() {
            group.clear();
            let mut queue = hold(&self.queues[index]);
            queue.rows_end = new_end;
            if data.is_some() {
                queue.synced_through = new_end;
            }
        }
        *hold(&self.spare_groups[index]) = Some(group);
        outcome
    }

    /// Check a slot has rows that haven't reached the kernel
    pub fn has_waiting_rows(&self, slot: OpenSlot) -> bool {
        let index = slot.0 as usize;
        if !hold(&self.queues[index]).filling.is_empty() {
            return true;
        }
        match hold(&self.spare_groups[index]).as_ref() {
            Some(group) => !group.is_empty(),
            None => true,
        }
    }
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
