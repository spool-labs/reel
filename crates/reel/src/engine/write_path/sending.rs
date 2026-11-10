//! Sending a slot's rows and windows to its files in turn, and the syncs a volume's policy asks for

use std::io::{Error, IoSlice};
use std::mem; use std::sync::atomic::Ordering;

use reel_api::maintenance_report::{JobKind, PauseCause, WriteSource};
use reel_api::store_error::StoreError;
use reel_api::units::{ByteCount, FileOffset};

use crate::config::SyncPolicy;
use crate::engine::engine_state::{EngineState, SlotPin};
use crate::engine::open_segment::{
    OpenSegment, RecordWindow,
};
use crate::engine::volume::Volume;
use crate::guard::hold;
use crate::index::open_slots::OpenSlot;
use crate::io::{Io, IoFile, IoTask};


impl<Backend: Io> EngineState<Backend> {
    /// Run the volume's sync policy after a write
    pub fn after_write(&self, volume: &Volume<Backend>, length: ByteCount) -> Result<(), StoreError> {
        self.note_written(WriteSource::Records, length);
        match volume.policy {
            SyncPolicy::EveryWrite => self.sync_volume(volume),
            SyncPolicy::EveryBytes(limit) => {
                let total = volume.bytes_since_sync.fetch_add(length.0, Ordering::AcqRel) + length.0;
                // past twice the limit a writer syncs itself, so unsynced bytes stay bounded when the row job falls behind
                if total >= 2 * limit.0 && volume.bytes_since_sync.swap(0, Ordering::AcqRel) >= 2 * limit.0 {
                    return self.sync_volume(volume);
                }
                // the writer that crosses the limit hands the sync to the row job and goes on
                if total >= limit.0 && total - length.0 < limit.0 {
                    volume.is_sync_due.store(true, Ordering::Release);
                    if let Some(task) = self.row_task.get() {
                        task.wake();
                    }
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
                self.pause_for(PauseCause::Sync);
                continue;
            }
            sync.is_running = true;
            let covers = sync.started;
            drop(sync);

            let running = SyncRun { volume };
            let outcome = self.run_sync(volume);
            drop(running);
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
        let volume = self.volumes.get(self.slot_files[slot.0 as usize].volume.load(Ordering::Acquire))?;
        let claims = hold(&volume.claims);
        let segment = claims.open.iter().find(|segment| segment.slot == slot)?;
        let pin = self.slot_files[slot.0 as usize].pin(segment.number)?;
        Some((pin, segment.rows.clone()))
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
            self.pause_for(PauseCause::WritingTurn);
        }
    }

    /// Take the slot's writing turn and send its rows, in a sync when given the data file
    pub fn write_out_turn(&self, slot: OpenSlot, rows: &Backend::File, data: Option<&Backend::File>) -> Result<(), Error> {
        self.take_writing_turn(slot);
        self.send_in_turn(slot, rows, data)
    }

    /// Send a slot's rows with its turn held, and give the turn back
    pub fn send_in_turn(&self, slot: OpenSlot, rows: &Backend::File, data: Option<&Backend::File>) -> Result<(), Error> {
        let _turn = WritingTurn { state: self, index: slot.0 as usize };
        let mut outcome = self.send_group(slot, rows, data);
        if outcome.is_ok() && self.has_left_over_rows(slot) {
            outcome = self.send_group(slot, rows, data);
        }
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
            self.note_written(WriteSource::Rows, ByteCount(bytes.len() as u64));
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

/// A slot's writing turn, given back however the send ends. A panic inside it fails every volume, since the slot's queue can
/// stand half changed and the next writer would carry on from there
pub struct WritingTurn<'state, Backend: Io> {
    pub state: &'state EngineState<Backend>,
    pub index: usize,
}

impl<Backend: Io> Drop for WritingTurn<'_, Backend> {
    fn drop(&mut self) {
        if std::thread::panicking() {
            for volume in &self.state.volumes {
                volume.fail();
            }
        }
        hold(&self.state.queues[self.index]).is_writing = false;
    }
}

/// A volume's sync under way. A panic inside it fails the volume and lets the next sync start, so the writers waiting on this
/// one hear the failure
pub struct SyncRun<'volume, Backend: Io> {
    pub volume: &'volume Volume<Backend>,
}

impl<Backend: Io> Drop for SyncRun<'_, Backend> {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.volume.fail();
            hold(&self.volume.sync).is_running = false;
        }
    }
}
