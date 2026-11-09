//! Puts, deletes and syncs

use std::hint;
use std::io::{Error, ErrorKind, IoSlice};
use std::mem;
use std::sync::Mutex;
use std::sync::atomic::Ordering;

use reel_api::column_id::ColumnId;
use reel_api::maintenance_report::JobKind;
use reel_api::store_error::StoreError;
use reel_api::units::{ByteCount, DeathMark, FileOffset, Nanoseconds, RootId, RowCount, WritePlace};

use crate::config::SyncPolicy;
use crate::engine::claim::Claim;
use crate::engine::column_info::ColumnInfo;
use crate::engine::ration::RATION_SPINS;
use crate::engine::engine_sizing::{OPEN_PER_ROOT, SEAL_ROWS_EIGHTHS};
use crate::engine::engine_state::{EngineState, IndexWait, SlotData, SlotPin};
use crate::engine::open_segment::{
    MOST_WINDOW_RECORD_BYTES, OpenSegment, RECORD_WINDOW_BYTES, RESERVE_STEP_BYTES, RecordWindow, RowQueue, SegmentPhase, data_path,
    rows_path,
};
use crate::engine::volume::{ClaimState, Volume};
use crate::guard::hold;
use crate::index::open_slots::OpenSlot;
use crate::index::write_row::WriteRow;
use crate::lifetime::{MOST_LIFETIME_GROUPS, MarkRange, ceiling_of, reach_of};
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
/// The job asks for a segment that takes writes as they come, with nothing claimed yet
pub const SPARE_REQUEST: ClaimRequest = ClaimRequest {
    bytes: ByteCount(0),
    rows: RowCount(0),
    is_alone: false,
    has_records: true,
    covers: 0,
    mark: None,
    has_deletes: false,
};
/// How many times a writer waits on the spot for a window to go out before it tries again
pub const WINDOW_SPINS: u32 = 2_000;
/// A writer waiting on a window spins this many rounds of spins, then pauses, since a sync holds the turn for milliseconds
pub const WINDOW_SPIN_ROUNDS: u32 = 4;
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
    pub mark: Option<DeathMark>,
    pub has_deletes: bool,
}

/// A value on its way to a record
#[derive(Clone, Copy, Debug)]
pub struct ValueWrite<'bytes> {
    pub column: ColumnId,
    pub key: &'bytes [u8],
    pub value: &'bytes [u8],
    pub codec: Codec,
    pub mark: Option<DeathMark>,
}

/// How an attempt to land a row ended
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LandOutcome {
    /// The row is in the index and the queue
    Landed,
    /// The index has no room, so nothing changed
    IndexFull,
}

/// Room held in a slot's filling group, given back when it drops, by which time a landed row has taken it
#[derive(Debug)]
pub struct RowRoom<'queue> {
    pub queue: &'queue Mutex<RowQueue>,
    pub size: usize,
}

impl Drop for RowRoom<'_> {
    fn drop(&mut self) {
        hold(self.queue).reserved -= self.size;
    }
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
    pub fn write_value(&self, column: ColumnId, key: &[u8], value: &[u8], mark: Option<DeathMark>) -> Result<(), StoreError> {
        let info = self.column(column)?;
        self.check_key(column, key)?;
        let volume = &self.volumes[info.volume.0 as usize];
        check_value_size(volume, value)?;
        self.note_value(column, &info, value);
        check_writable(volume)?;
        self.draw_ration(volume, RowCount(1))?;

        let write = ValueWrite {
            column,
            key,
            value,
            codec: info.codec,
            mark,
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
            mark: write.mark,
            has_deletes: false,
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
    /// left for a window. Writers go on in the new window while this one sends the old
    pub fn refill_window(
        &self,
        volume: &Volume<Backend>,
        slot: OpenSlot,
        length: usize,
        size: usize,
    ) -> Result<bool, StoreError> {
        let index = slot.0 as usize;
        let sent = |outcome: Result<(), Error>| {
            outcome.map_err(|source| StoreError::Io {
                action: "writing rows",
                source,
            })
        };
        {
            let guard = hold(&self.queues[index]);
            if !guard.filling.has_room(guard.reserved + size) {
                drop(guard);
                sent(self.write_out(slot))?;
                return Ok(true);
            }
        }

        match self.switch_window(volume, slot, length) {
            WindowSwitch::HasRoom => Ok(true),
            WindowSwitch::NoRoom => Ok(false),
            WindowSwitch::Switched => {
                // the new window has room, and whoever holds the turn or the row job sends the old one
                if self.try_writing_turn(slot) {
                    sent(self.send_kept_in_turn(index))?;
                }
                Ok(true)
            },
            WindowSwitch::OtherBusy => {
                sent(self.wait_for_kept_window(slot))?;
                Ok(true)
            },
        }
    }

    /// Wait for the window waiting to go, holding nothing, and send it when the turn comes free
    pub fn wait_for_kept_window(&self, slot: OpenSlot) -> Result<(), Error> {
        let index = slot.0 as usize;
        let mut rounds = 0;
        loop {
            if self.try_writing_turn(slot) {
                return self.send_kept_in_turn(index);
            }
            if !hold(&self.queues[index]).has_kept_window {
                return Ok(());
            }
            if rounds < WINDOW_SPIN_ROUNDS {
                // the write that holds the turn takes microseconds, against a millisecond for any sleep
                rounds += 1;
                for _spin in 0..WINDOW_SPINS {
                    hint::spin_loop();
                }
            } else {
                self.io.pause();
            }
        }
    }

    /// Send the window waiting to go with the slot's turn held, and give the turn back
    pub fn send_kept_in_turn(&self, index: usize) -> Result<(), Error> {
        let outcome = self.send_kept_window(index);
        hold(&self.queues[index]).is_writing = false;
        outcome
    }

    /// Move a slot to the next range of its segment in the other window, under the locks and with no I/O
    pub fn switch_window(&self, volume: &Volume<Backend>, slot: OpenSlot, length: usize) -> WindowSwitch {
        let index = slot.0 as usize;
        let mut claims = hold(&volume.claims);
        let Some(segment) = claims.open.iter_mut().find(|segment| segment.slot == slot) else {
            // the segment went while no claim was held, and the next claim finds another
            return WindowSwitch::HasRoom;
        };
        if segment.phase != SegmentPhase::Taking {
            return WindowSwitch::HasRoom;
        }

        let mut guard = hold(&self.queues[index]);
        let live = guard.live_window;
        if self.windows[index][live].used() + length <= guard.window_room {
            return WindowSwitch::HasRoom;
        }
        if guard.has_kept_window {
            return WindowSwitch::OtherBusy;
        }
        let left = volume.segment_bytes.0.saturating_sub(segment.write_end.0) as usize;
        let room = left.min(RECORD_WINDOW_BYTES);
        if room < length {
            return WindowSwitch::NoRoom;
        }

        let start = segment.write_end;
        segment.write_end = FileOffset(start.0 + room as u64);
        self.windows[index][1 - live].reset(start);
        guard.live_window = 1 - live;
        guard.window_room = room;
        guard.has_kept_window = !self.windows[index][live].is_empty();
        WindowSwitch::Switched
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
            mark: write.mark,
            has_deletes: false,
        };
        loop {
            let (claim, room) = self.claim_with_room(volume, &request, row_size(write.key.len()))?;
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
            let landed = self.land_reserved(write.column, write.key, &row);
            drop(room);
            match landed {
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
        if self.index.is_newest_grave(column, key, self.read_point(volume), volume.view()) {
            return Ok(());
        }

        self.draw_ration(volume, RowCount(1))?;
        let request = ClaimRequest {
            bytes: ByteCount(0),
            rows: RowCount(1),
            is_alone: false,
            has_records: false,
            covers: 0,
            mark: None,
            has_deletes: true,
        };
        loop {
            let (claim, room) = self.claim_with_room(volume, &request, row_size(key.len()))?;
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
            let landed = self.land_reserved(column, key, &row);
            drop(room);
            match landed {
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
        self.draw_ration(volume, RowCount(1))?;
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
            mark: None,
            has_deletes: true,
        };
        loop {
            let (claim, room) = self.claim_with_room(volume, &request, row_size(start.len()))?;
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
            let landed = self.land_reserved(column, start, &row);
            drop(room);
            match landed {
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
                    self.drop_covers_inside(volume, &cover);
                    return Ok(length);
                },
                LandOutcome::IndexFull => {
                    drop(claim);
                    self.wait_for_index(column, start, slot)?;
                },
            }
        }
    }

    /// Count a value under the least its column declares. It lands as any other, and only leaves its volume's row floor high
    pub fn note_value(&self, column: ColumnId, info: &ColumnInfo, value: &[u8]) {
        if (value.len() as u64) < info.least_value {
            self.values_under_least[column.0 as usize].fetch_add(1, Ordering::Relaxed);
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

    /// Read the rows the write index holds, or will once every claim lands
    pub fn rows_held(&self) -> RowCount {
        RowCount(self.volumes.iter().map(|volume| volume.rows_in_index.load(Ordering::Relaxed)).sum())
    }

    /// Read the rows the write index plans for, of which a segment takes its limit
    pub fn index_capacity(&self) -> RowCount {
        RowCount(self.sizing.segment_rows.0 * 8 / SEAL_ROWS_EIGHTHS)
    }

    /// Wait for a volume's seal to reach the point where these rows fit its ration, in line behind any writer already waiting.
    /// Each volume answers only to its own seal, so a meta writer goes past a bulk seal's walk. It waits behind that seal only
    /// once the index itself is full, and the bulk writers stop short of that by the share held back for the seal's last steps
    pub fn draw_ration(&self, volume: &Volume<Backend>, rows: RowCount) -> Result<(), StoreError> {
        if volume.ration.lets_through(self.rows_held(), rows, || self.io.now()) {
            return Ok(());
        }
        check_seal_room(volume)?;

        let started = self.io.now();
        self.index_waits.fetch_add(1, Ordering::Relaxed);
        let _waiting = IndexWait::new(&self.index_waiters);
        let deadline = started.0.saturating_add(INDEX_WAIT.0);
        let ticket = volume.ration.take_ticket();
        let mut spins = 0;
        loop {
            if volume.ration.is_served(ticket) {
                let is_clear = volume.ration.line(|| self.io.now()).is_none_or(|line| self.rows_held().0 + rows.0 <= line.0);
                let has_failed = volume.has_failed();
                let is_late = self.io.now().0 > deadline;
                if is_clear || has_failed || is_late {
                    volume.ration.next_in_line();
                    self.longest_index_wait.fetch_max(self.io.now().0.saturating_sub(started.0), Ordering::Relaxed);
                    check_writable(volume)?;
                    if !is_clear {
                        return Err(StoreError::Io {
                            action: "waiting for the write index",
                            source: Error::from(ErrorKind::TimedOut),
                        });
                    }
                    return Ok(());
                }
            }
            spins += 1;
            if spins < RATION_SPINS {
                hint::spin_loop();
            } else {
                spins = 0;
                self.io.pause();
            }
        }
    }

    /// Claim with room held for rows in the group filling. A full group goes out with no claim held, since a writer
    /// that writes with a ticket stops the mark for every writer behind it
    pub fn claim_with_room<'state>(
        &'state self,
        volume: &'state Volume<Backend>,
        request: &ClaimRequest,
        size: usize,
    ) -> Result<(Claim<'state, Backend>, RowRoom<'state>), StoreError> {
        loop {
            let claim = self.claim(volume, request)?;
            let queue = &self.queues[claim.slot.0 as usize];
            {
                let mut guard = hold(queue);
                if guard.filling.has_room(guard.reserved + size) {
                    guard.reserved += size;
                    drop(guard);
                    return Ok((claim, RowRoom { queue, size }));
                }
            }

            let slot = claim.slot;
            drop(claim);
            self.write_out(slot).map_err(|source| StoreError::Io {
                action: "writing rows",
                source,
            })?;
        }
    }

    /// Put a row in the write index and the rows queue, or in neither, with room held for it
    pub fn land_reserved(&self, column: ColumnId, key: &[u8], row: &WriteRow) -> LandOutcome {
        if !self.index.insert(column, key, row) {
            return LandOutcome::IndexFull;
        }

        let entry = RowEntry {
            column,
            key,
            place: row.place,
            offset: row.offset,
            length: row.length,
            kind: row.kind,
        };
        // the reservation holds this room, whichever group is filling by now
        hold(&self.queues[row.slot.0 as usize]).filling.push(&entry);
        self.wake_row_job();
        LandOutcome::Landed
    }

    /// Put a row in the write index and the rows queue, or in neither, for a caller with no claim to give up
    pub fn land_row(&self, column: ColumnId, key: &[u8], row: &WriteRow) -> Result<LandOutcome, StoreError> {
        let size = row_size(key.len());
        self.reserve_row(row.slot, size)?;
        let room = RowRoom {
            queue: &self.queues[row.slot.0 as usize],
            size,
        };
        let landed = self.land_reserved(column, key, row);
        drop(room);
        Ok(landed)
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
        let _waiting = IndexWait::new(&self.index_waiters);
        let deadline = self.io.now().0.saturating_add(INDEX_WAIT.0);
        let own = &self.volumes[self.column(column)?.volume.0 as usize];
        loop {
            for volume in &self.volumes {
                self.seal_fullest(volume);
            }
            if self.index.has_room(column, key, slot) {
                self.longest_index_wait.fetch_max(self.io.now().0.saturating_sub(started.0), Ordering::Relaxed);
                return Ok(());
            }
            check_seal_room(own)?;
            if self.io.now().0 > deadline {
                return Err(StoreError::Io {
                    action: "waiting for the write index",
                    source: Error::from(ErrorKind::TimedOut),
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
            let current = DeathMark(self.current_mark.load(Ordering::Relaxed));
            let pick = pick_segment(&claims, volume, request, current);
            if let Some(index) = pick.to_seal {
                self.seal_at(&mut claims, volume, index);
            }
            if pick.must_wait {
                drop(claims);
                check_seal_room(volume)?;
                self.io.pause();
                continue;
            }
            if pick.wants_more || pick.best.is_none() {
                // a segment made ahead costs a claim nothing, and the job starts on the next
                if let Some(mut segment) = claims.prepared.take() {
                    segment.ceiling = pick.ceiling;
                    volume.open_slot(segment.slot);
                    claims.open.push(segment);
                    self.ask_for_segment(&mut claims);
                    continue;
                }
                // no segment seals on its own for covers that fit none, and each holds a slot
                if pick.best.is_none() && request.covers > 0 {
                    self.seal_most_covers(&mut claims, volume);
                }
                if pick.wants_more {
                    self.ask_for_segment(&mut claims);
                }
            }

            if let Some(index) = pick.best {
                // the reserve job keeps a step held ahead, and a claim past what it holds takes the room itself
                let segment = &mut claims.open[index];
                let end = segment.write_end.0 + request.bytes.0;
                if end > segment.reserved_end.0 {
                    let target = reserve_target(segment, volume, end);
                    if let Err(source) = segment.data.reserve_to(ByteCount(target)) {
                        // a full disk seals the segment, so no claim tries it again
                        self.seal_at(&mut claims, volume, index);
                        return Err(store_error(volume, "reserving a segment's room", source));
                    }
                    segment.reserved_end = FileOffset(target);
                    segment.is_reserve_asked = false;
                }
                let is_short = segment.reserved_end.0 < volume.segment_bytes.0 && segment.reserved_end.0.saturating_sub(end) < RESERVE_STEP_BYTES / 2;
                if !segment.is_reserve_asked && is_short {
                    segment.is_reserve_asked = true;
                    if let Some(task) = self.reserve_task.get() {
                        task.wake();
                    }
                }

                let Some(ticket) = volume.order.try_claim(request.rows) else {
                    // the ring frees when claims end, and ending one needs this lock, and so does a raise
                    drop(claims);
                    if self.places.is_past_limit(request.rows) {
                        let _is_held = self.reserve_ahead();
                    }
                    self.io.pause();
                    continue;
                };

                let segment = &mut claims.open[index];
                if request.is_alone {
                    segment.phase = SegmentPhase::Alone;
                }
                // the seal measures the ingest period from the first row, one clock read a segment
                if segment.first_row_at.is_none() {
                    segment.first_row_at = Some(self.io.now());
                }
                let offset = segment.write_end;
                segment.write_end = FileOffset(offset.0 + request.bytes.0);
                segment.row_count = RowCount(segment.row_count.0 + request.rows.0);
                volume.rows_in_index.fetch_add(request.rows.0, Ordering::Relaxed);
                segment.in_flight += 1;
                segment.cover_rows += request.covers;
                segment.has_deletes |= request.has_deletes;
                segment.lowest_place = segment.lowest_place.min(ticket.first);
                segment.highest_place = segment.highest_place.max(ticket.last);
                if let Some(mark) = request.mark {
                    segment.marks.add(mark);
                }
                let number = segment.number;
                let data = segment.data.clone();
                let slot = segment.slot;
                self.slot_files[slot.0 as usize].pins.fetch_add(1, Ordering::AcqRel);
                self.mark_full_segments(&mut claims, volume);
                drop(claims);
                // a group goes out with the marks so far, and this comes before the row lands
                if let Some(mark) = request.mark {
                    hold(&self.queues[slot.0 as usize]).marks.add(mark);
                }
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
                Ok(Some(mut segment)) => {
                    segment.ceiling = pick.ceiling;
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

        // half full is time for the job to have the next one ready before this one seals
        let is_half_full = rows * 2 >= volume.segment_rows.0 || bytes * 2 >= volume.segment_bytes.0;
        if is_half_full && claims.prepared.is_none() {
            self.ask_for_segment(claims);
        }

        let point = volume.seal_point.load(Ordering::Relaxed);
        if rows >= point || bytes >= volume.segment_bytes.0 {
            self.mark_fullest(claims, volume);
        }
    }

    /// Mark the taking segment with the most covers to seal, unless a seal is on its way to free a slot
    pub fn seal_most_covers(&self, claims: &mut ClaimState<Backend::File>, volume: &Volume<Backend>) {
        if claims.open.iter().any(|segment| segment.phase == SegmentPhase::Sealing) {
            return;
        }
        let taking = claims.open.iter_mut().filter(|segment| segment.phase == SegmentPhase::Taking);
        if let Some(segment) = taking.max_by_key(|segment| segment.cover_rows) {
            segment.phase = SegmentPhase::Sealing;
            if let Some(task) = self.seal_tasks[volume.id.0 as usize].get() {
                task.wake();
            }
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
    /// disk leaves none ready, and the writer that needs one hears it when it creates its own. A spare holds a step of
    /// disk until a claim takes it or close removes it
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

            let created = self.create_segment(volume, RootId(root as u8), &SPARE_REQUEST);
            self.finish_create(volume, created);
        }
    }

    /// Take a made segment in as the spare. It answers every ask so far, and the claim that takes it asks again
    pub fn finish_create(&self, volume: &Volume<Backend>, created: Result<Option<OpenSegment<Backend::File>>, StoreError>) {
        let mut claims = hold(&volume.claims);
        claims.is_creating = false;
        match created {
            Ok(Some(segment)) => {
                claims.prepared = Some(segment);
                claims.wants_prepared = false;
            },
            // left asked for, so the job tries at its next period or when a seal frees a slot, and no claim wakes it
            Ok(None) | Err(_) => claims.wants_prepared = true,
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
        // a data file left with no rows and no tail reads as damage, so the rows go only once it has
        if self.remove_or_park(volume, root, segment.number) {
            self.io.remove_file(&rows_path(root, segment.number))?;
        }
        self.io.sync_directory(root)
    }

    /// Mark one open segment to seal, with the claim lock held
    pub fn seal_at(&self, claims: &mut ClaimState<Backend::File>, volume: &Volume<Backend>, index: usize) {
        claims.open[index].phase = SegmentPhase::Sealing;
        if let Some(task) = self.seal_tasks[volume.id.0 as usize].get() {
            task.wake();
        }
    }

    /// Take the current mark, and seal every open group it has reached, since a group left open would keep its records
    /// past the slack they were promised
    pub fn set_current_mark(&self, mark: DeathMark) {
        self.current_mark.store(mark.0, Ordering::Relaxed);
        for volume in &self.volumes {
            let mut claims = hold(&volume.claims);
            for index in 0..claims.open.len() {
                let segment = &claims.open[index];
                if segment.phase == SegmentPhase::Taking && segment.ceiling.is_some_and(|ceiling| ceiling <= mark) {
                    self.seal_at(&mut claims, volume, index);
                }
            }
        }
        // a segment past its highest mark drops to the plain bar, and nothing else would look
        self.ask_compaction();
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
            ByteCount(RESERVE_STEP_BYTES.max(request.bytes.0).min(volume.segment_bytes.0.max(request.bytes.0)))
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
                let _is_gone = self.remove_or_park(volume, path, number);
                return Err(store_error(volume, "creating a rows file", source));
            },
        };

        if let Err(source) = self.io.sync_directory(path) {
            self.io.close_file(data);
            self.io.close_file(rows);
            if self.remove_or_park(volume, path, number) {
                let _removed = self.io.remove_file(&rows_file);
            }
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
            reserved_end: FileOffset(reserved.0),
            is_reserve_asked: false,
            row_count: RowCount(0),
            in_flight: 0,
            cover_rows: 0,
            lowest_place: WritePlace(u64::MAX),
            highest_place: WritePlace(0),
            ceiling: None,
            marks: MarkRange::NONE,
            has_deletes: false,
            seen_rows: RowCount(0),
            quiet_since: Nanoseconds(0),
            first_row_at: None,
            delete_seen: None,
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

        let (rows_end, synced_through, mut marked_end, marks) = {
            let mut queue = hold(&self.queues[index]);
            // a group a failed write left behind goes first, and whole
            if group.is_empty() {
                mem::swap(&mut queue.filling, &mut group);
            }
            (queue.rows_end, queue.synced_through, queue.marked_end, queue.marks)
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
            let bytes = group.finish(synced_through, marks, self.record_key);
            let parts = [IoSlice::new(bytes)];
            outcome = rows.write_at(rows_end, &parts);
            new_end = FileOffset(rows_end.0 + bytes.len() as u64);
            self.note_moved(JobKind::RowWriteOut, ByteCount(bytes.len() as u64));
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
                    let bytes = group.finish(covered, marks, self.record_key);
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

    /// Send the records of a slot to the kernel: any window waiting to go, then the live one, whose rest of the range
    /// goes on in the other window. Each round decides under one hold, so a writer's switch can't slip in between
    pub fn send_windows(&self, index: usize) -> Result<(), Error> {
        let windows = &self.windows[index];
        loop {
            let is_live = {
                let mut queue = hold(&self.queues[index]);
                let is_live = !queue.has_kept_window;
                if is_live {
                    let live = queue.live_window;
                    if windows[live].is_empty() {
                        return Ok(());
                    }
                    windows[1 - live].reset(windows[live].end());
                    queue.window_room -= windows[live].used();
                    queue.live_window = 1 - live;
                    queue.has_kept_window = true;
                }
                is_live
            };
            self.send_kept_window(index)?;
            // the group's rows were fixed before this call, so the live window held the last of their records
            if is_live {
                return Ok(());
            }
        }
    }

    /// Send the window waiting to go, which takes the slot's writing turn
    pub fn send_kept_window(&self, index: usize) -> Result<(), Error> {
        let (window, is_kept) = {
            let queue = hold(&self.queues[index]);
            (&self.windows[index][1 - queue.live_window], queue.has_kept_window)
        };
        if !is_kept {
            return Ok(());
        }

        self.send_window(index, window)?;
        // reads go to the file from here, before any sync, so a sync never holds one up
        window.reset(FileOffset(0));
        hold(&self.queues[index]).has_kept_window = false;
        Ok(())
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

/// What a try to move a slot to its next window found
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WindowSwitch {
    /// The live window has room, or the slot has left this segment, so the next claim goes on
    HasRoom,
    /// The slot moved on, and the old window is waiting to go
    Switched,
    /// The other window is still going out
    OtherBusy,
    /// Too little of the segment is left for a window
    NoRoom,
}

/// A claim's choice of segment
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SegmentPick {
    pub best: Option<usize>,
    pub wants_more: bool,
    /// The ceiling a segment opened for this claim takes
    pub ceiling: Option<DeathMark>,
    /// The segment to seal first, to make room for a group
    pub to_seal: Option<usize>,
    /// Every group is busy, so a new one waits for a seal
    pub must_wait: bool,
}

/// Check a segment can take a claim
pub fn has_room<Backend: Io>(segment: &OpenSegment<Backend::File>, volume: &Volume<Backend>, request: &ClaimRequest) -> bool {
    let has_bytes = segment.write_end.0 + request.bytes.0 <= volume.segment_bytes.0.max(request.bytes.0);
    let has_rows = segment.row_count.0 + request.rows.0 <= volume.segment_rows.0.max(request.rows.0);
    // a seal lists a segment's covers in a buffer sized for this many
    let has_cover_room = segment.cover_rows + request.covers <= MOST_RANGE_COVERS;
    has_bytes && has_rows && has_cover_room
}

/// Pick the segment for a claim: its lifetime group when it has a mark, and otherwise the unmarked one with the fewest writers
pub fn pick_segment<Backend: Io>(
    claims: &ClaimState<Backend::File>,
    volume: &Volume<Backend>,
    request: &ClaimRequest,
    current: DeathMark,
) -> SegmentPick {
    match request.mark {
        Some(mark) => pick_group(claims, volume, request, mark, current),
        None => pick_unmarked(claims, volume, request),
    }
}

/// Pick the taking unmarked segment with the fewest writers, and say when the volume should open another
pub fn pick_unmarked<Backend: Io>(
    claims: &ClaimState<Backend::File>,
    volume: &Volume<Backend>,
    request: &ClaimRequest,
) -> SegmentPick {
    let mut best: Option<usize> = None;
    let mut taking = 0;
    for (index, segment) in claims.open.iter().enumerate() {
        if segment.phase != SegmentPhase::Taking || segment.ceiling.is_some() || !has_room(segment, volume, request) {
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
    SegmentPick {
        best,
        wants_more,
        ceiling: None,
        to_seal: None,
        must_wait: false,
    }
}

/// Pick the open group with the lowest ceiling a mark may join, or say what a new group needs first
pub fn pick_group<Backend: Io>(
    claims: &ClaimState<Backend::File>,
    volume: &Volume<Backend>,
    request: &ClaimRequest,
    mark: DeathMark,
    current: DeathMark,
) -> SegmentPick {
    let reach = reach_of(mark, current);
    let (mut home, mut dies_first, mut groups) = (None, None, 0);
    for (index, segment) in claims.open.iter().enumerate() {
        let Some(ceiling) = segment.ceiling else {
            continue;
        };
        if segment.phase == SegmentPhase::Sealing {
            continue;
        }
        groups += 1;
        if segment.phase != SegmentPhase::Taking {
            continue;
        }
        if dies_first.is_none_or(|(first, _index)| ceiling < first) {
            dies_first = Some((ceiling, index));
        }
        if ceiling >= mark && ceiling <= reach && home.is_none_or(|(lowest, _index)| ceiling < lowest) {
            home = Some((ceiling, index));
        }
    }

    let mut pick = SegmentPick {
        best: None,
        wants_more: false,
        ceiling: Some(ceiling_of(mark, current)),
        to_seal: None,
        must_wait: false,
    };
    match home {
        Some((_ceiling, index)) if has_room(&claims.open[index], volume, request) => pick.best = Some(index),
        // a full group gives up its place to the one that follows it
        Some((_ceiling, index)) => pick.to_seal = Some(index),
        None if groups < MOST_LIFETIME_GROUPS => {},
        None => {
            pick.to_seal = dies_first.map(|(_ceiling, index)| index);
            pick.must_wait = dies_first.is_none();
        },
    }
    pick
}

/// Count the lifetime groups open now
pub fn lifetime_groups<File>(claims: &ClaimState<File>) -> usize {
    claims.open.iter().filter(|segment| segment.ceiling.is_some() && segment.phase != SegmentPhase::Sealing).count()
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

/// Refuse a wait that only a seal could end while the volume's seal stands stopped on a full table
pub fn check_seal_room<Backend: Io>(volume: &Volume<Backend>) -> Result<(), StoreError> {
    if volume.is_seal_stuck.load(Ordering::Acquire) {
        return Err(StoreError::TableFull { volume: volume.id });
    }
    Ok(())
}

/// Work out the end a segment's room grows to for a claim that ends at `end`: a step past what it holds, up to the segment's
/// bytes or the claim's end, whichever is further
pub fn reserve_target<Backend: Io>(segment: &OpenSegment<Backend::File>, volume: &Volume<Backend>, end: u64) -> u64 {
    let limit = volume.segment_bytes.0.max(end);
    (segment.reserved_end.0 + RESERVE_STEP_BYTES).max(end).min(limit)
}
