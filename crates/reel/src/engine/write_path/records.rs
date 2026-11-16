//! Writing values, graves and range deletes: each record lands in a window and its row waits in the slot's queue

use std::hint;
use std::io::{Error, IoSlice};
use std::sync::atomic::Ordering;

use reel_core::maintenance_report::PauseCause;
use reel_core::column_id::ColumnId;
use reel_core::store_error::StoreError;
use reel_core::units::{ByteCount, DeathMark, FileOffset, RowCount, WritePlace};

use crate::engine::claim::Claim;
use crate::engine::column_info::ColumnInfo;
use crate::engine::engine_state::EngineState;
use crate::engine::open_segment::{
    MOST_WINDOW_RECORD_BYTES, RECORD_WINDOW_BYTES, SegmentPhase,
};
use crate::engine::volume::Volume;
use crate::guard::hold;
use crate::index::open_slots::OpenSlot;
use crate::index::write_row::WriteRow;
use crate::io::{Io, IoFile};
use crate::record::codec::{CHUNK_BYTES, Codec};
use crate::record::record_check::RecordIdentity;
use crate::record::record_encoder::{PlainRecord, RecordEncoder, RecordShape};
use crate::record::record_head::plain_length;
use crate::row_kind::RowKind;
use crate::rows::row_group::{RowEntry, row_size};
use crate::segment::range_covers::Cover;

use crate::engine::write_path::{ClaimRequest, LandOutcome, ValueWrite, WINDOW_SPINS, WINDOW_SPIN_ROUNDS, WindowSwitch, WritingTurn, check_value_size, check_writable, store_error};

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
                self.pause_for(PauseCause::RecordTurn);
            }
        }
    }

    /// Send the window waiting to go with the slot's turn held, and give the turn back
    pub fn send_kept_in_turn(&self, index: usize) -> Result<(), Error> {
        let _turn = WritingTurn { state: self, index };
        self.send_kept_window(index)
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
}
