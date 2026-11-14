//! Scans: merge the sealed segments and the write index in key order, and answer each key at the newest point or a cue's

use std::cmp::Ordering as KeyOrder;
use std::io::Error;
use std::mem;
use std::sync::atomic::Ordering;

use reel_api::column_id::ColumnId;
use reel_api::direction::Direction;
use reel_api::scan::ScanStep;
use reel_api::store_error::StoreError;
use reel_api::units::{ByteCount, FileOffset, RowCount, WritePlace};

use crate::engine::engine_state::EngineState;
use crate::engine::read_path::{HeldPin, KeyRead, Located, MOST_READ_TRIES, RecordWant, SealedRow, newest_of};
use crate::engine::scan_state::{
    BATCH_BYTES, BATCH_KEYS, BatchEntry, BlockTake, JOIN_GAP, MOST_BATCH_RECORD, MOST_SCANS, MOST_WINDOW_RECORD, NO_GENERATION, ScanRange, ScanState, ScanWindows, SegmentSource, under_prefix,
};
use crate::engine::volume::Volume;
use crate::guard::hold;
use crate::index::write_shard::WalkBound;
use crate::io::{DeviceReads, Io, ManyRead, ReadOn, in_place};
use crate::record::record_check::RecordIdentity;
use crate::record::codec::Codec;
use crate::record::record_reader::{RecordRead, form_of, open_small};
use crate::row_kind::RowKind;
use crate::segment::footer_reader::RowAt;
use crate::segment::block_cache::BlockKey;
use crate::segment::block_entry::{BlockEntry, KeyBound, SHARED_HEAD_BYTES};
use crate::segment::segment_table::Seat;
use crate::segment::column_directory::ColumnDirectory;
use crate::segment::damage_list::KeyRange;
use crate::segment::segment_tail::{ColumnTail, SegmentNumber};

/// What a source found when it moved
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SourceMove {
    /// It stands on a row
    Row,
    /// It has no row left in the walk's direction
    Done,
    /// Its segment left, and the list that took it out has its rows
    Left,
}

/// What one try at a step came to
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StepOutcome {
    /// The step has its answer
    Answer(ScanStep),
    /// The key has no value at this point, and the walk has passed it
    Passed,
    /// A job crossed the try, and the walk seeks again from its last key
    Moved,
}

/// Where a source seats itself
#[derive(Clone, Copy, Debug)]
pub struct SeatAt<'key> {
    pub column: ColumnId,
    pub key: &'key [u8],
    pub is_inclusive: bool,
    pub direction: Direction,
    pub point: WritePlace,
    pub is_counting: bool,
}

/// What a step does with the key it lands on
#[derive(Debug)]
pub enum ScanWant<'value> {
    /// Count it
    Key,
    /// Read its value
    Value(&'value mut Vec<u8>),
    /// Keep its row in the scan's batch with no value read
    Locate,
    /// Add up the bytes its value takes on disk
    Bytes,
}

/// Which total a walk over a prefix adds up
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ScanWantKind {
    /// Keys
    Key,
    /// Stored bytes
    Bytes,
}

/// What a step keeps while it answers a key
#[derive(Clone, Copy)]
pub struct StepAt<'list> {
    pub directory: Option<&'list ColumnDirectory>,
    pub generation: u64,
    pub fixed_point: Option<WritePlace>,
}

/// Where a walk's key sits among one column's keys
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ColumnSpot {
    /// It starts with the shared bytes, so the block index can place it
    Inside,
    /// Every key of the column comes after it in bytes
    Below,
    /// Every key of the column comes before it in bytes
    Above,
}

impl<Backend: Io> EngineState<Backend> {
    /// Take a scan's buffers from the pool and start it
    pub fn take_scan(&self, range: &ScanRange, point: Option<WritePlace>) -> Result<ScanState, StoreError> {
        self.column(range.column)?;
        let Some(mut scan) = hold(&self.read_pool.scans).pop() else {
            return Err(StoreError::ScansFull { limit: MOST_SCANS as u64 });
        };
        scan.start(range, point);
        Ok(scan)
    }

    /// Hand a scan's buffers back
    pub fn give_back_scan(&self, scan: ScanState) {
        hold(&self.read_pool.scans).push(scan);
    }

    /// Take one step of a scan on this thread, through the ring as a blocking get reads, with a value buffer when it wants values.
    /// A job that crosses a try costs a seek from the last key, and past the usual tries a pin holds the jobs off that key
    pub fn step_scan(&self, scan: &mut ScanState, key: &mut Vec<u8>, want: ScanWant) -> Result<ScanStep, StoreError> {
        in_place(self.step_scan_on(&self.blocking_reads(), scan, key, want))
    }

    /// Take one step of a scan through a device's read path
    pub async fn step_scan_on(
        &self,
        device: &impl DeviceReads<Backend::File>,
        scan: &mut ScanState,
        key: &mut Vec<u8>,
        mut want: ScanWant<'_>,
    ) -> Result<ScanStep, StoreError> {
        let info = self.column(scan.column)?;
        let volume = &self.volumes[info.volume.0 as usize];
        let directory = self.directories[scan.column.0 as usize].as_ref();
        // a walk for values answers a batch of keys located at one point, so a write landing while it lasts shows from the next
        if let ScanWant::Value(value) = &mut want {
            if !scan.batch.has_next() {
                self.fill_batch(device, volume, directory, scan).await?;
            }
            if let Some(step) = self.answer_from_batch(ReadOn::new(device, volume), scan, key, value).await? {
                return Ok(step);
            }
        }
        let mut tries = 0;
        loop {
            if self.is_stopping.load(Ordering::Acquire) {
                return Err(StoreError::Closed);
            }
            let many = if tries >= MOST_READ_TRIES && scan.point.is_none() { self.many_read(volume) } else { None };
            let pinned = many.and_then(|many| many.pin.map(|slot| (many, slot)));
            let held = pinned.map(|(many, slot)| HeldPin { state: self, slot, counts: many.counts });
            let outcome = self.try_step(ReadOn::new(device, volume), directory, scan, pinned.map(|(many, _slot)| many.point), key, want.reborrow()).await;
            drop(held);
            match self.counted(outcome)? {
                StepOutcome::Answer(step) => {
                    self.corruption_errors.fetch_add(u64::from(step == ScanStep::Corrupt), Ordering::Relaxed);
                    return Ok(step);
                },
                StepOutcome::Passed => {},
                StepOutcome::Moved => {
                    tries += 1;
                    scan.generation = NO_GENERATION;
                    if tries > 2 * MOST_READ_TRIES {
                        return self.refuse_read();
                    }
                },
            }
        }
    }

    /// Try one key: seat what moved, find the walk's next key, and answer it
    pub async fn try_step(
        &self,
        on: ReadOn<'_, impl DeviceReads<Backend::File>, Volume<Backend>>,
        directory: Option<&ColumnDirectory>,
        scan: &mut ScanState,
        pin_point: Option<WritePlace>,
        key: &mut Vec<u8>,
        want: ScanWant<'_>,
    ) -> Result<StepOutcome, StoreError> {
        let ReadOn { device, target: volume } = on;
        let generation = directory.map_or(0, ColumnDirectory::generation);
        let fixed_point = scan.point.or(pin_point);
        let point = fixed_point.unwrap_or_else(|| self.read_point(volume));
        if generation != scan.generation {
            // a block taken by its line has no key to seek from, so a count that took one starts over and steps every row
            if scan.taken > 0 {
                scan.restart_count();
            }
            scan.reseat(generation);
        }
        self.place_heads(volume, scan);
        if let Some(directory) = directory {
            self.join_segments(device, volume, directory, scan, point).await?;
        }

        let mut candidate = mem::take(&mut scan.key);
        candidate.clear();
        let mut has_key = scan.closest_key().is_some_and(|closest| {
            candidate.extend_from_slice(closest);
            true
        });
        let spilled = match directory.filter(|_directory| scan.is_spilled) {
            Some(directory) => self.spilled_key(on, directory, scan, point, has_key, &mut candidate).await,
            None => Ok(has_key),
        };
        has_key = match spilled {
            Ok(has_key) => has_key && !scan.is_past_limit(&candidate),
            Err(error) => {
                scan.key = candidate;
                return Err(error);
            },
        };

        let outcome = match self.check_reach(volume, scan, has_key.then_some(&candidate)) {
            Ok(()) if has_key => {
                let at = StepAt { directory, generation, fixed_point };
                self.answer_key(on, scan, &candidate, at, key, want).await
            },
            Ok(()) => Ok(StepOutcome::Answer(ScanStep::End)),
            Err(error) => Err(error),
        };
        scan.key = candidate;
        outcome
    }

    /// Answer the walk's next key at a point
    pub async fn answer_key(
        &self,
        on: ReadOn<'_, impl DeviceReads<Backend::File>, Volume<Backend>>,
        scan: &mut ScanState,
        candidate: &[u8],
        at: StepAt<'_>,
        key: &mut Vec<u8>,
        want: ScanWant<'_>,
    ) -> Result<StepOutcome, StoreError> {
        let ReadOn { device, target: volume } = on;
        // the counts come before the point, as a get takes them
        let counts = self.cues.counts();
        let read = match at.fixed_point {
            Some(point) => self.key_read(scan.column, candidate, point, true)?,
            None => self.key_read_now(scan.column, candidate)?,
        };
        let is_pinned = at.fixed_point.is_some();
        // a key no head shows has no index row at a point the heads already saw
        let found = match scan.is_off_the_heads(candidate, read.read_point) {
            true => None,
            false => {
                scan.lookups += 1;
                self.index.lookup(scan.column, candidate, read.read_point, volume.view()).row
            },
        };
        let taken = self.take_rows(device, volume, scan, candidate, read.read_point).await?;
        let sealed = if scan.is_spilled { self.search_sealed(device, volume, &read).await? } else { taken };
        // a seal moves rows from the index to a segment this walk hasn't seated, and both of its looks must see one list
        let is_same_list = at.directory.is_none_or(|directory| directory.generation() == at.generation);
        if !is_same_list {
            return Ok(StepOutcome::Moved);
        }

        let located = newest_of(found, sealed);
        let is_hidden = located.place().is_some_and(|place| read.cover.is_some_and(|cover| cover > place));
        if !located.has_value() || is_hidden {
            scan.pass(candidate);
            return Ok(StepOutcome::Passed);
        }

        let answer = match want {
            ScanWant::Value(value) => self.read_located(on, &read, &located, &mut scan.window, scan.direction, value).await,
            ScanWant::Key => {
                scan.counted += 1;
                Ok(true)
            },
            ScanWant::Locate => {
                let key_at = scan.batch.keys.len();
                scan.batch.keys.extend_from_slice(candidate);
                scan.batch.entries.push(BatchEntry { key_at, key_length: candidate.len(), located, record: None });
                Ok(true)
            },
            ScanWant::Bytes => {
                scan.counted += located.length().0;
                Ok(true)
            },
        };
        let is_bad = matches!(answer, Err(StoreError::Corruption { .. }));
        // a free can only fail a record check: its row was replaced, and the next try finds the replacement
        if is_bad && !is_pinned && !self.has_outlived(counts, true) {
            return Ok(StepOutcome::Moved);
        }
        let step = match answer {
            Ok(true) => ScanStep::Found,
            Ok(false) => return Ok(StepOutcome::Moved),
            Err(StoreError::Corruption { .. }) => ScanStep::Corrupt,
            Err(error) => return Err(error),
        };
        key.clear();
        key.extend_from_slice(candidate);
        scan.pass(candidate);
        Ok(StepOutcome::Answer(step))
    }

    /// Locate the walk's next keys at one point with no value read, then read their small sealed records in joined runs
    pub async fn fill_batch(
        &self,
        device: &impl DeviceReads<Backend::File>,
        volume: &Volume<Backend>,
        directory: Option<&ColumnDirectory>,
        scan: &mut ScanState,
    ) -> Result<(), StoreError> {
        scan.batch.clear();
        let point = scan.point.unwrap_or_else(|| self.read_point(volume));
        scan.batch.point = point;
        let mut key = mem::take(&mut scan.out_key);
        let mut outcome = Ok(());
        // a batch takes keys while the records it reads still fit its buffer, so wide records read in batches of fewer keys
        let mut located_bytes = 0;
        while scan.batch.entries.len() < BATCH_KEYS && located_bytes + MOST_BATCH_RECORD <= BATCH_BYTES {
            let entries = scan.batch.entries.len();
            match self.counted(self.try_step(ReadOn::new(device, volume), directory, scan, Some(point), &mut key, ScanWant::Locate).await) {
                Ok(StepOutcome::Answer(ScanStep::Found) | StepOutcome::Passed) => {
                    if let Some(BatchEntry { located: Located::Sealed(row), .. }) = scan.batch.entries.get(entries)
                        && row.row.length.0 as usize <= MOST_BATCH_RECORD
                    {
                        located_bytes += row.row.length.0 as usize;
                    }
                },
                // the step that answers one key at a time takes the end, a crossed try and anything it can't locate
                Ok(StepOutcome::Moved) => {
                    scan.generation = NO_GENERATION;
                    break;
                },
                Ok(StepOutcome::Answer(_step)) => break,
                Err(error) => {
                    outcome = Err(error);
                    break;
                },
            }
        }
        scan.out_key = key;
        // keys located before a failure answer first, and the next fill meets the failure again
        if scan.batch.entries.is_empty() {
            outcome?;
        }
        self.read_batch(device, volume, scan).await;
        Ok(())
    }

    /// Read the batch's sealed records together. A record left out takes its own read when its key is answered
    pub async fn read_batch(&self, device: &impl DeviceReads<Backend::File>, volume: &Volume<Backend>, scan: &mut ScanState) {
        let batch = &mut scan.batch;
        let mut rows = [None; BATCH_KEYS];
        for (row, entry) in rows.iter_mut().zip(&batch.entries) {
            if let Located::Sealed(sealed) = entry.located {
                *row = Some(sealed);
            }
        }
        let mut spans = [None; BATCH_KEYS];
        let count = batch.entries.len();
        self.read_records(device, volume, &rows[..count], &mut batch.records, &mut spans).await;
        for (entry, span) in batch.entries.iter_mut().zip(spans) {
            if span.is_some() {
                entry.record = span;
            }
        }
    }

    /// Read small sealed records in segment and offset order, one read for each run of neighbours, with every run sent before
    /// any is waited for. Each record read lands in the buffer and its span says where, and a record left out has no span
    pub async fn read_records(
        &self,
        device: &impl DeviceReads<Backend::File>,
        volume: &Volume<Backend>,
        rows: &[Option<SealedRow>],
        records: &mut Vec<u8>,
        spans: &mut [Option<(usize, usize)>],
    ) {
        let small = |index: u16| rows[index as usize].filter(|row| row.row.length.0 as usize <= MOST_BATCH_RECORD);
        let mut order = [0_u16; BATCH_KEYS];
        let mut count = 0;
        for index in 0..rows.len().min(BATCH_KEYS) as u16 {
            if small(index).is_some() {
                order[count] = index;
                count += 1;
            }
        }
        let order = &mut order[..count];
        order.sort_unstable_by_key(|index| small(*index).map(|row| (row.segment, row.row.offset.0)));
        // each run: its first and last place in the order, its file offset and its length
        let mut runs = [(0_usize, 0_usize, 0_u64, 0_u64); BATCH_KEYS];
        let mut pins: [Option<_>; BATCH_KEYS] = std::array::from_fn(|_| None);
        let (mut run_count, mut planned) = (0, 0_u64);
        let room = (records.capacity() - records.len()) as u64;
        let mut at = 0;
        while at < order.len() {
            let Some(first) = small(order[at]) else {
                break;
            };
            let start = first.row.offset.0;
            let mut end = start + first.row.length.0;
            let mut last = at;
            let left = room - planned;
            while let Some(next) = order.get(last + 1).and_then(|index| small(*index)) {
                let next_end = next.row.offset.0 + next.row.length.0;
                if next.segment != first.segment || next.row.offset.0 > end + JOIN_GAP || next_end.max(end) - start > left {
                    break;
                }
                end = end.max(next_end);
                last += 1;
            }
            if end - start > left {
                break;
            }
            if let Some(pin) = volume.table.pin(first.segment) {
                runs[run_count] = (at, last, start, end - start);
                pins[run_count] = Some(pin);
                run_count += 1;
                planned += end - start;
            }
            at = last + 1;
        }
        let base = records.len();
        records.resize(base + planned as usize, 0);
        let mut reads: [Option<ManyRead<'_, Backend::File>>; BATCH_KEYS] = std::array::from_fn(|_| None);
        let mut rest = &mut records[base..];
        for ((read, &(_at, _last, start, length)), pin) in reads.iter_mut().zip(&runs).zip(&pins).take(run_count) {
            let (buf, tail) = mem::take(&mut rest).split_at_mut(length as usize);
            rest = tail;
            *read = pin.as_ref().map(|pin| ManyRead { file: &pin.file, offset: FileOffset(start), buf, is_done: false });
        }
        device.read_many(&mut reads[..run_count]).await;
        let filled: [bool; BATCH_KEYS] = std::array::from_fn(|index| reads[index].as_ref().is_some_and(|read| read.is_done));
        let mut from = base;
        for (&(at, last, start, length), is_filled) in runs.iter().zip(filled).take(run_count) {
            if is_filled {
                for &index in &order[at..=last] {
                    if let Some(row) = small(index) {
                        spans[index as usize] = Some((from + (row.row.offset.0 - start) as usize, row.row.length.0 as usize));
                    }
                }
            }
            from += length as usize;
        }
    }

    /// Answer the batch's next key from its record, or by its own read when the batch holds none. Nothing when the batch is
    /// empty, or its row moved on, and then the walk seeks again from that key
    pub async fn answer_from_batch(
        &self,
        on: ReadOn<'_, impl DeviceReads<Backend::File>, Volume<Backend>>,
        scan: &mut ScanState,
        key: &mut Vec<u8>,
        value: &mut Vec<u8>,
    ) -> Result<Option<ScanStep>, StoreError> {
        let ScanState { batch, window, direction, column, .. } = scan;
        let Some(entry) = batch.entries.get(batch.next).copied() else {
            return Ok(None);
        };
        let read = self.key_read(*column, batch.key_of(&entry), batch.point, true)?;
        let held = match (entry.record, entry.located) {
            (Some((at, length)), Located::Sealed(row)) => self.open_batched(on.device, &read, &row, &batch.records[at..at + length], value),
            _other => false,
        };
        let is_found = held
            || match self.read_located(on, &read, &entry.located, window, *direction, value).await {
                Ok(is_found) => is_found,
                Err(StoreError::Corruption { .. }) => false,
                Err(error) => return Err(error),
            };
        if !is_found {
            scan.position.clear();
            scan.position.extend_from_slice(scan.batch.key_of(&entry));
            scan.is_inclusive = true;
            scan.generation = NO_GENERATION;
            scan.batch.clear();
            return Ok(None);
        }
        key.clear();
        key.extend_from_slice(scan.batch.key_of(&entry));
        scan.batch.next += 1;
        scan.batched += u64::from(held);
        Ok(Some(ScanStep::Found))
    }

    /// Check and open a small record the batch read, as the window does
    pub fn open_batched(&self, device: &impl DeviceReads<Backend::File>, read: &KeyRead<'_>, sealed: &SealedRow, record: &[u8], value: &mut Vec<u8>) -> bool {
        let identity = RecordIdentity {
            record_key: self.record_key,
            column: read.column,
            key: read.key,
            kind: RowKind::Value,
            place: sealed.row.place,
        };
        let Some(form) = form_of(record).filter(|form| !form.is_chunked) else {
            return false;
        };
        let opened = match form.codec {
            Codec::None => open_small(record, form, &identity, &mut [], value),
            Codec::Lz4 => self.record_reader_for(device).check_held(record, form, &identity, value),
        };
        opened == RecordRead::Valid
    }

    /// Read the value of the row a key landed on, and return false when its segment or slot moved on
    pub async fn read_located(
        &self,
        on: ReadOn<'_, impl DeviceReads<Backend::File>, Volume<Backend>>,
        read: &KeyRead<'_>,
        located: &Located,
        window: &mut ScanWindows,
        direction: Direction,
        value: &mut Vec<u8>,
    ) -> Result<bool, StoreError> {
        let ReadOn { device, target: volume } = on;
        match located {
            Located::Absent => Ok(false),
            Located::Open(row) => self.read_open(device, read, row, RecordWant::Whole, value).await,
            Located::Sealed(row) => {
                if let Some(answer) = self.read_from_window(on, read, row, window, direction, value).await {
                    return Ok(answer);
                }
                self.read_sealed(device, volume, read, row, RecordWant::Whole, value).await
            },
        }
    }

    /// Answer a small plain record from the scan's window, reading the next stretch when it lacks it. Anything else comes back
    /// as nothing for the record's own read to settle: a bad stretch can be a neighbour's, and a failed check says nothing
    /// about this row until its own read does
    #[allow(clippy::too_many_arguments, reason = "a read, its row, and the window and direction a scan keeps")]
    pub async fn read_from_window(
        &self,
        on: ReadOn<'_, impl DeviceReads<Backend::File>, Volume<Backend>>,
        read: &KeyRead<'_>,
        sealed: &SealedRow,
        window: &mut ScanWindows,
        direction: Direction,
        value: &mut Vec<u8>,
    ) -> Option<bool> {
        let ReadOn { device, target: volume } = on;
        let (offset, length) = (sealed.row.offset.0, sealed.row.length.0 as usize);
        if length > MOST_WINDOW_RECORD {
            return None;
        }
        if window.holding(sealed.segment, offset, length).is_none() {
            let pin = volume.table.pin(sealed.segment)?;
            let (at, bytes) = window.plan(sealed.segment, (offset, length), direction, pin.head.data_end.0);
            if device.read_exact(&pin.file, at, bytes).await.is_err() {
                window.clear();
                return None;
            }
        }
        let identity = RecordIdentity {
            record_key: self.record_key,
            column: read.column,
            key: read.key,
            kind: RowKind::Value,
            place: sealed.row.place,
        };
        let record = window.holding(sealed.segment, offset, length)?;
        let form = form_of(record).filter(|form| !form.is_chunked)?;
        let read = match form.codec {
            Codec::None => open_small(record, form, &identity, &mut [], value),
            Codec::Lz4 => self.record_reader_for(device).check_held(record, form, &identity, value),
        };
        match read {
            RecordRead::Valid => Some(true),
            RecordRead::Failed => None,
        }
    }

    /// Refuse a step that reaches a damaged span, since a row in it could hide or replace anything the walk would say next
    pub fn check_reach(&self, volume: &Volume<Backend>, scan: &ScanState, next: Option<&Vec<u8>>) -> Result<(), StoreError> {
        if volume.damage.is_empty() {
            return Ok(());
        }
        let limit = scan.has_limit.then_some(scan.limit.as_slice());
        let range = match scan.direction {
            Direction::Ascending => KeyRange {
                low: &scan.position,
                high: next.map(Vec::as_slice).or(limit),
                is_high_inclusive: next.is_some(),
            },
            Direction::Descending => KeyRange {
                low: next.map(Vec::as_slice).or(limit).unwrap_or_default(),
                high: Some(&scan.position),
                is_high_inclusive: scan.is_inclusive,
            },
        };
        if volume.damage.meets(scan.column, &range) {
            return Err(StoreError::Corruption { column: scan.column });
        }
        Ok(())
    }

    /// Bring each shard whose head was used up to its next key
    pub fn place_heads(&self, volume: &Volume<Backend>, scan: &mut ScanState) {
        let bound = WalkBound {
            column: scan.column,
            key: &scan.position,
            is_inclusive: scan.is_inclusive,
            direction: scan.direction,
        };
        // every place at or below the mark is in the index before the heads look
        let mark = self.read_point(volume);
        let view = volume.view();
        let is_new_view = view != scan.heads_view;
        scan.heads_view = view;
        let mut has_moved = false;
        for (shard, head) in scan.heads.iter_mut().enumerate() {
            if !head.is_placed || is_new_view {
                head.has_key = self.index.next_key(shard, &mut head.cursor, &bound, view, &mut head.key);
                head.is_placed = true;
                head.placed_at = mark;
                has_moved = true;
            }
        }
        if has_moved {
            scan.find_closest_head();
        }
    }

    /// Seat every segment that starts before the walk's next key, one bound of the list at a time
    pub async fn join_segments(
        &self,
        device: &impl DeviceReads<Backend::File>,
        volume: &Volume<Backend>,
        directory: &ColumnDirectory,
        scan: &mut ScanState,
        point: WritePlace,
    ) -> Result<(), StoreError> {
        while let Some(frontier) = scan.frontier {
            let closest = scan.closest_key().map(KeyBound::of_key);
            let is_ahead = closest.is_some_and(|bound| match scan.direction {
                Direction::Ascending => bound < frontier,
                Direction::Descending => bound > frontier,
            });
            if is_ahead {
                return Ok(());
            }
            if scan.is_bound_past_limit(&frontier) {
                scan.frontier = None;
                return Ok(());
            }
            let reach = closest.unwrap_or(frontier);
            let (low, high) = match scan.direction {
                Direction::Ascending => (frontier, reach),
                Direction::Descending => (reach, frontier),
            };
            self.join_range(ReadOn::new(device, volume), directory, scan, &low, &high, point).await?;
            scan.frontier = match scan.direction {
                Direction::Ascending => directory.next_above(&reach),
                Direction::Descending => directory.next_below(&reach),
            };
        }
        Ok(())
    }

    /// Seat every segment a range of bounds meets. What the sources can't hold spills, and goes by seeks from then on
    pub async fn join_range(
        &self,
        on: ReadOn<'_, impl DeviceReads<Backend::File>, Volume<Backend>>,
        directory: &ColumnDirectory,
        scan: &mut ScanState,
        low: &KeyBound,
        high: &KeyBound,
        point: WritePlace,
    ) -> Result<(), StoreError> {
        let ReadOn { device, target: volume } = on;
        let mut skip = 0;
        loop {
            let page = directory.overlapping(low, high, skip, &mut scan.found);
            skip += scan.found.len();
            for index in 0..scan.found.len() {
                let segment = scan.found[index];
                if scan.holds(segment) {
                    continue;
                }
                if scan.source_count == scan.sources.len() {
                    scan.is_spilled = true;
                    continue;
                }
                // a segment gone from under the list is in the next one, which the step's last look will see
                if self.seat_listed(ReadOn::new(device, volume), scan, segment, point, false).await? == SourceMove::Row {
                    scan.source_count += 1;
                    scan.is_tree_stale = true;
                }
            }
            if !page.is_cut {
                return Ok(());
            }
        }
    }

    /// Find the walk's next key among the segments the sources couldn't hold, and say whether it has one
    pub async fn spilled_key(
        &self,
        on: ReadOn<'_, impl DeviceReads<Backend::File>, Volume<Backend>>,
        directory: &ColumnDirectory,
        scan: &mut ScanState,
        point: WritePlace,
        has_key: bool,
        candidate: &mut Vec<u8>,
    ) -> Result<bool, StoreError> {
        let ReadOn { device, target: volume } = on;
        let mut has_key = has_key;
        let position = KeyBound::of_key(&scan.position);
        let reach = if has_key { KeyBound::of_key(candidate) } else { scan.far_bound() };
        let (low, high) = match scan.direction {
            Direction::Ascending => (position, reach),
            Direction::Descending => (reach, position),
        };
        let mut skip = 0;
        loop {
            let page = directory.overlapping(&low, &high, skip, &mut scan.found);
            skip += scan.found.len();
            for index in 0..scan.found.len() {
                let segment = scan.found[index];
                if scan.holds(segment) {
                    continue;
                }
                if self.seat_listed(ReadOn::new(device, volume), scan, segment, point, true).await? == SourceMove::Row
                    && (!has_key || scan.is_before(&scan.spill.key, candidate))
                {
                    candidate.clear();
                    candidate.extend_from_slice(&scan.spill.key);
                    has_key = true;
                }
            }
            if !page.is_cut {
                return Ok(has_key);
            }
        }
    }

    /// Seat a listed segment at the walk's place, in the next free source or in the spill slot
    pub async fn seat_listed(
        &self,
        on: ReadOn<'_, impl DeviceReads<Backend::File>, Volume<Backend>>,
        scan: &mut ScanState,
        segment: SegmentNumber,
        point: WritePlace,
        is_spill: bool,
    ) -> Result<SourceMove, StoreError> {
        let ScanState { column, position, is_inclusive, direction, is_counting, sources, spill, source_count, .. } = scan;
        let at = SeatAt {
            column: *column,
            key: position,
            is_inclusive: *is_inclusive,
            direction: *direction,
            point,
            is_counting: *is_counting,
        };
        let source = if is_spill { spill } else { &mut sources[*source_count] };
        source.segment = segment;
        self.seat_source(on.device, on.target, source, &at).await
    }

    /// Walk every source past a key, and return its newest row at a point
    pub async fn take_rows(
        &self,
        device: &impl DeviceReads<Backend::File>,
        volume: &Volume<Backend>,
        scan: &mut ScanState,
        key: &[u8],
        point: WritePlace,
    ) -> Result<Option<SealedRow>, StoreError> {
        let mut best: Option<SealedRow> = None;
        let (column, direction) = (scan.column, scan.direction);
        // the tree's closest source stands on the key while any does, and one that moves past it plays its matches again
        loop {
            scan.settle_tree();
            let Some(index) = scan.closest_source().filter(|index| scan.sources[*index].key == key) else {
                return Ok(best);
            };
            let mut take = (scan.is_counting && !scan.is_spilled).then(|| BlockTake { gate: scan.gate_without(index), rows: 0 });
            let source = &mut scan.sources[index];
            let mut step = SourceMove::Row;
            while step == SourceMove::Row && source.key == key {
                if source.row.place <= point && best.is_none_or(|row| row.row.place < source.row.place) {
                    best = Some(SealedRow {
                        segment: source.segment,
                        row: source.row,
                        block_offset: source.line.block_offset,
                        at: RowAt::UNKNOWN,
                    });
                }
                step = match self.step_source(ReadOn::new(device, volume), column, source, direction, point, take.as_mut()).await {
                    Ok(step) => step,
                    // a source that failed partway may have moved, and the tree builds again before it picks
                    Err(error) => {
                        scan.is_tree_stale = true;
                        return Err(error);
                    },
                };
            }
            let taken = take.map_or(0, |take| take.rows);
            scan.counted += taken;
            scan.taken += taken;
            match step {
                SourceMove::Row => scan.replay(index),
                // a segment leaves its list before the table, so the step's look at the list sees this
                SourceMove::Done | SourceMove::Left => scan.drop_source(index),
            }
        }
    }

    /// Move a source to its next row in the walk's direction
    pub async fn step_source(
        &self,
        on: ReadOn<'_, impl DeviceReads<Backend::File>, Volume<Backend>>,
        column: ColumnId,
        source: &mut SegmentSource,
        direction: Direction,
        point: WritePlace,
        mut take: Option<&mut BlockTake>,
    ) -> Result<SourceMove, StoreError> {
        let ReadOn { device, target: volume } = on;
        loop {
            let row = match self.hold_block(device, volume, column, source).await? {
                Some(true) => Some(source.step_held(direction)),
                Some(false) => {
                    source.cache_steps += 1;
                    let (segment, line) = (source.segment, source.line);
                    self.visit_block(device, volume, column, segment, &line, |block| match direction {
                        Direction::Ascending => source.next_in_block(block),
                        Direction::Descending => source.previous_in_block(block),
                    }).await?
                },
                None => None,
            };
            match row {
                None => return Ok(SourceMove::Left),
                Some(Some(row)) => {
                    source.row = row;
                    return Ok(SourceMove::Row);
                },
                Some(None) => {
                    let crossed = self.cross_block(volume, source, direction, point, take.as_deref_mut());
                    if crossed != SourceMove::Row {
                        return Ok(crossed);
                    }
                },
            }
        }
    }

    /// Enter the next block with a live row. A block with none is crossed unread once its replacements stand at or below the
    /// point: a row it holds was replaced by one the walk finds in another block, or a cover hides it
    pub fn cross_block(
        &self,
        volume: &Volume<Backend>,
        source: &mut SegmentSource,
        direction: Direction,
        point: WritePlace,
        mut take: Option<&mut BlockTake>,
    ) -> SourceMove {
        loop {
            match direction {
                Direction::Ascending => {
                    source.block += 1;
                    if source.block >= source.end_block {
                        return SourceMove::Done;
                    }
                },
                Direction::Descending => {
                    if source.block == source.first_block {
                        return SourceMove::Done;
                    }
                    source.block -= 1;
                },
            }
            let Some((line, replacement)) = volume.table.block_line(source.segment, source.block) else {
                return SourceMove::Left;
            };
            if let Some(take) = take.as_deref_mut()
                && self.can_take_line(volume, source, take)
            {
                take.rows += u64::from(line.live_rows);
                continue;
            }
            if line.live_rows > 0 || replacement > point {
                source.enter_block(line);
                return SourceMove::Row;
            }
        }
    }

    /// Stand a source on its segment's first row past a key
    pub async fn seat_source(
        &self,
        device: &impl DeviceReads<Backend::File>,
        volume: &Volume<Backend>,
        source: &mut SegmentSource,
        at: &SeatAt<'_>,
    ) -> Result<SourceMove, StoreError> {
        let seat = match volume.table.seat_line(source.segment, at.column, at.key, at.direction) {
            Seat::Left => return Ok(SourceMove::Left),
            Seat::NoColumn => return Ok(SourceMove::Done),
            Seat::Line(seat) => seat,
        };
        let tail = seat.tail;
        source.place_base = seat.head.place_base;
        let can_take = at.is_counting && seat.is_walked && seat.head.places.highest <= at.point;
        source.is_countable = can_take && has_no_live_graves(volume, source.segment, at.column);
        source.tail = tail;
        source.first_block = tail.first_block;
        source.end_block = tail.first_block + tail.block_count.0;
        let (start, line, replacement) = match (seat.is_settled, seat.start) {
            (true, Some((start, line))) => (start, line, seat.replacement),
            (true, None) => return Ok(SourceMove::Done),
            // a key that matches more shared bytes than the tail holds takes the column's first row to place
            (false, _start) => {
                let spot = self.column_spot(device, volume, source, &tail, at).await?;
                let start = match (spot, at.direction) {
                    (ColumnSpot::Inside, direction) => volume.table.entry_block(source.segment, &tail, at.key, direction),
                    (ColumnSpot::Below, Direction::Ascending) => Some(source.first_block),
                    (ColumnSpot::Above, Direction::Descending) => source.end_block.checked_sub(1),
                    (ColumnSpot::Below, Direction::Descending) | (ColumnSpot::Above, Direction::Ascending) => None,
                };
                let Some(start) = start.filter(|_start| tail.block_count.0 > 0) else {
                    return Ok(SourceMove::Done);
                };
                let Some((line, replacement)) = volume.table.block_line(source.segment, start) else {
                    return Ok(SourceMove::Left);
                };
                (start, line, replacement)
            },
        };
        source.block = start;
        source.enter_block(line);
        if line.live_rows == 0 && replacement <= at.point {
            let crossed = self.cross_block(volume, source, at.direction, at.point, None);
            if crossed != SourceMove::Row {
                return Ok(crossed);
            }
        }
        loop {
            let (segment, line) = (source.segment, source.line);
            let row = self.visit_block(device, volume, at.column, segment, &line, |block| match at.direction {
                Direction::Ascending => source.seek_up(block, at.key, at.is_inclusive),
                Direction::Descending => source.seek_down(block, at.key, at.is_inclusive),
            }).await?;
            match row {
                None => return Ok(SourceMove::Left),
                Some(Some(row)) => {
                    source.row = row;
                    return Ok(SourceMove::Row);
                },
                Some(None) => {
                    let crossed = self.cross_block(volume, source, at.direction, at.point, None);
                    if crossed != SourceMove::Row {
                        return Ok(crossed);
                    }
                },
            }
        }
    }

    /// Check a block can count by its line: its span ends before anything else the walk knows of, so its live rows are keys
    /// no other row has a say in
    pub fn can_take_line(
        &self,
        volume: &Volume<Backend>,
        source: &SegmentSource,
        take: &BlockTake,
    ) -> bool {
        if !source.is_countable {
            return false;
        }
        let next = source.block + 1;
        let after = (next < source.end_block).then(|| volume.table.block_line(source.segment, next)).flatten();
        let upper = after.map_or(source.tail.highest, |(line, _replacement)| line.first_key);
        source.tail.bound_above(upper) < take.gate
    }

    /// Count the keys under a prefix at a point, with a block inside it taken by its line where that is exact
    pub fn count_at(&self, column: ColumnId, prefix: &[u8], point: WritePlace) -> Result<RowCount, StoreError> {
        let info = self.column(column)?;
        let volume = &self.volumes[info.volume.0 as usize];
        let mut scan = self.take_scan(&under_prefix(column, prefix), Some(point))?;
        scan.is_counting = volume.covers.is_empty() && volume.damage.is_empty();
        let outcome = self.walk_prefix(&mut scan, ScanWantKind::Key);
        let count = RowCount(scan.counted);
        self.give_back_scan(scan);
        outcome.map(|()| count)
    }

    /// Add up the stored bytes of the values under a prefix at a point, stepping rows and reading no record
    pub fn bytes_at(&self, column: ColumnId, prefix: &[u8], point: WritePlace) -> Result<ByteCount, StoreError> {
        let mut scan = self.take_scan(&under_prefix(column, prefix), Some(point))?;
        let outcome = self.walk_prefix(&mut scan, ScanWantKind::Bytes);
        let bytes = ByteCount(scan.counted);
        self.give_back_scan(scan);
        outcome.map(|()| bytes)
    }

    /// Walk a prefix to its end, leaving the total in the scan
    pub fn walk_prefix(&self, scan: &mut ScanState, kind: ScanWantKind) -> Result<(), StoreError> {
        let mut key = mem::take(&mut scan.out_key);
        let outcome = loop {
            let want = match kind {
                ScanWantKind::Key => ScanWant::Key,
                ScanWantKind::Bytes => ScanWant::Bytes,
            };
            match self.step_scan(scan, &mut key, want) {
                Ok(ScanStep::Found | ScanStep::Corrupt) => {},
                Ok(ScanStep::End) => break Ok(()),
                Err(error) => break Err(error),
            }
        };
        scan.out_key = key;
        outcome
    }

    /// Place a walk's key against the bytes a column's keys share, which the block index leaves out. A prefix longer than the
    /// tail holds takes the column's first key to settle
    pub async fn column_spot(
        &self,
        device: &impl DeviceReads<Backend::File>,
        volume: &Volume<Backend>,
        source: &mut SegmentSource,
        tail: &ColumnTail,
        at: &SeatAt<'_>,
    ) -> Result<ColumnSpot, StoreError> {
        let shared = tail.shared_length as usize;
        let mut order = tail.order_against_shared(at.key);
        if order == KeyOrder::Equal && shared > SHARED_HEAD_BYTES {
            let Some((line, _replacement)) = volume.table.block_line(source.segment, tail.first_block) else {
                return Ok(ColumnSpot::Inside);
            };
            source.enter_block(line);
            let segment = source.segment;
            let first = self.visit_block(device, volume, at.column, segment, &line, |block| source.next_in_block(block)).await?;
            if first.flatten().is_some() {
                order = at.key[..at.key.len().min(shared)].cmp(&source.key[..shared]);
            }
        }
        Ok(match order {
            KeyOrder::Less => ColumnSpot::Below,
            KeyOrder::Equal => ColumnSpot::Inside,
            KeyOrder::Greater => ColumnSpot::Above,
        })
    }

    /// Copy a source's block into its own buffer once, and say whether it holds it there. A source with no buffer, or a block
    /// too long for it, goes through the cache. Nothing when the segment has left
    pub async fn hold_block(
        &self,
        device: &impl DeviceReads<Backend::File>,
        volume: &Volume<Backend>,
        column: ColumnId,
        source: &mut SegmentSource,
    ) -> Result<Option<bool>, StoreError> {
        let at = (source.segment, source.line.block_offset);
        if source.held_at == Some(at) {
            return Ok(Some(true));
        }
        let length = source.line.block_length().0 as usize;
        if length > source.held.capacity() {
            return Ok(Some(false));
        }
        let held = &mut source.held;
        let line = source.line;
        let copied = self.visit_block(device, volume, column, at.0, &line, |block| {
            held.clear();
            held.extend_from_slice(block);
        }).await?;
        if copied.is_none() {
            return Ok(None);
        }
        source.held_at = Some(at);
        source.fills += 1;
        Ok(Some(true))
    }

    /// Run a function over a row block through the cache, and return nothing when its segment has left
    pub async fn visit_block<Answer>(
        &self,
        device: &impl DeviceReads<Backend::File>,
        volume: &Volume<Backend>,
        column: ColumnId,
        segment: SegmentNumber,
        line: &BlockEntry,
        mut visit: impl FnMut(&[u8]) -> Answer + Send,
    ) -> Result<Option<Answer>, StoreError> {
        let key = BlockKey {
            segment,
            offset: line.block_offset,
        };
        if let Some(answer) = self.row_cache.with(key, &mut visit) {
            return Ok(Some(answer));
        }
        let Some(pin) = volume.table.pin(segment) else {
            return Ok(None);
        };
        let mut reader = self.footer_reader_for(device);
        match reader.load_block(device, &pin.file, key, line, &self.row_cache).await {
            Ok(Some(block)) => Ok(Some(visit(block))),
            Ok(None) => Err(StoreError::Corruption { column }),
            Err(source) => Err(read_error(source)),
        }
    }
}

impl ScanWant<'_> {
    /// Lend this for one try
    pub fn reborrow(&mut self) -> ScanWant<'_> {
        match self {
            ScanWant::Key => ScanWant::Key,
            ScanWant::Value(value) => ScanWant::Value(value),
            ScanWant::Bytes => ScanWant::Bytes,
            ScanWant::Locate => ScanWant::Locate,
        }
    }
}

/// Check a segment's row says a column holds no live grave and no cover, so a block's live rows are all values
pub fn has_no_live_graves<Backend: Io>(volume: &Volume<Backend>, segment: SegmentNumber, column: ColumnId) -> bool {
    let rows = hold(&volume.summary);
    let Some(row) = rows.get(segment).filter(|row| !row.facts.has_covers) else {
        return false;
    };
    let mut cursor = row.cursor();
    for _column in 0..row.column_count {
        let Some(facts) = cursor.column() else {
            return false;
        };
        if facts.column == column {
            return facts.grave_count.0 <= facts.dead_graves.0;
        }
    }
    false
}

/// Report a failed read of a row block
pub fn read_error(source: Error) -> StoreError {
    StoreError::Io {
        action: "reading a row block",
        source,
    }
}
