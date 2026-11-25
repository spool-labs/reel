//! Scans: merge the sealed segments and the write index in key order, and answer each key at the newest point or a cue's

use std::cmp::Ordering as KeyOrder;
use std::io::Error;
use std::mem;
use std::sync::RwLockReadGuard; use std::sync::atomic::Ordering;

use reel_core::column_id::ColumnId;
use reel_core::direction::Direction;
use reel_core::scan::ScanStep;
use reel_core::store_error::StoreError;
use reel_core::units::{ByteCount, FileOffset, RowCount, WritePlace};

use crate::engine::engine_state::{EngineState, SlotPin};
use crate::engine::open_segment::MOST_WINDOW_RECORD_BYTES;
use crate::engine::read_path::{HeldPin, KeyRead, Located, MOST_READ_TRIES, RecordWant, SealedRow, newest_located, newest_of};
use crate::engine::scan_state::{
    BatchEntry, BatchRow, BlockTake, JOIN_GAP, MOST_BATCH_RECORD, MOST_BATCH_RUNS, MOST_WINDOW_RECORD, NO_GENERATION, NO_CHANGES, ScanBatch, batch_room,
    SCAN_CACHE_PARTS, SEAT_BATCH, ScanRange, ScanState, ScanWindows, SegmentSource, ShardHead,
    FIRST_UNTOLD_KEYS, KeyFrom, SCAN_BATCH_KEYS, held_in, index_of_word, key_in, seek_together, under_prefix,
};
use crate::engine::volume::Volume;
use crate::guard::hold;
use crate::index::sorted_keys::{LeafAt, ScanFrom, SortedKeys};
use crate::config::KeyShape;
use crate::index::open_counts::OpenBound;
use crate::pool::grow_into_room;
use crate::pool_room::RoomRead;
use crate::index::write_shard::{HEAD_GROUP, HeadPlace, WalkBound, WriteShard, place_heads_together};
use crate::io::{DeviceReads, Io, ManyRead, ReadOn, in_place};
use crate::record::record_check::RecordIdentity;
use crate::record::codec::Codec;
use crate::record::record_head::SMALL_HEAD_BYTES;
use crate::record::lz4_block::decompress;
use crate::record::record_reader::{RecordRead, checked_payload, form_of, open_small, trusted_payload};
use crate::index::value_pages::MOST_HELD_VALUE;
use crate::row_kind::RowKind;
use crate::segment::footer_reader::RowAt;
use crate::index::record_cache::CacheLookup;
use crate::segment::block_cache::{BlockKey, NO_BLOCK};
use crate::segment::block_entry::{BlockEntry, KeyBound, SHARED_HEAD_BYTES};
use crate::segment::segment_table::{Seat, SegmentPin};
use crate::index::entry_slab::EntryAddress;
use crate::index::key_hash::hash_key;
use crate::index::write_row::WriteRow;
use crate::index::write_index::SHARD_COUNT;
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

/// Where a seat starts in its segment, worked out with no read of the device
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SeatPlace {
    /// The seat ended before any block: the segment left, or holds nothing past the key
    Moved(SourceMove),
    /// The seat starts in this block
    Block { start: u32, line: BlockEntry, replacement: WritePlace },
    /// The column's keys share more bytes than the tail holds, so the column's first row has to place the key
    Unsettled,
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
    /// Answer it with no value read, as of the batch's point
    Key,
    /// Count it, for a count over a prefix
    Count,
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
        let Some(mut scan) = self.read_pool.scans.take() else {
            return Err(StoreError::ScansFull { limit: self.read_pool.scan_count as u64 });
        };
        scan.start(range, point);
        Ok(scan)
    }

    /// Hand a scan's buffers back, with the counts it kept
    pub fn give_back_scan(&self, mut scan: ScanState) {
        self.add_scan_counts(&mut scan);
        self.read_pool.scans.give_back(scan);
    }

    /// Add the counts a scan kept to the store's totals
    pub fn add_scan_counts(&self, scan: &mut ScanState) {
        if scan.sorted_scan_keys > 0 {
            self.sorted_scan_keys.fetch_add(mem::take(&mut scan.sorted_scan_keys), Ordering::Relaxed);
        }
        if scan.corrupt_steps > 0 {
            self.corruption_errors.fetch_add(mem::take(&mut scan.corrupt_steps), Ordering::Relaxed);
        }
        if scan.held_values > 0 {
            self.scan_held_values.fetch_add(mem::take(&mut scan.held_values), Ordering::Relaxed);
        }
    }

    /// Count the step a batch answers, and locate the next batch when this one is used up, reading its records for a walk that
    /// wants values. The caller's key buffer holds each key the locate steps on, since the answer writes it after
    pub async fn ready_batch(
        &self,
        device: &impl DeviceReads<Backend::File>,
        volume: &Volume<Backend>,
        directory: Option<&ColumnDirectory>,
        scan: &mut ScanState,
        (key, is_reading): (&mut Vec<u8>, bool),
    ) -> Result<(), StoreError> {
        scan.asked += 1;
        if scan.batch.has_next() {
            return Ok(());
        }
        self.add_scan_counts(scan);
        self.fill_batch(device, volume, directory, scan, (key, is_reading)).await
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
        // a walk for values or keys answers a batch of keys located at one point, so a write landing while it lasts shows from the next
        let answer = match &mut want {
            ScanWant::Value(value) => {
                self.ready_batch(device, volume, directory, scan, (key, true)).await?;
                self.answer_from_batch(ReadOn::new(device, volume), scan, key, value).await?
            },
            ScanWant::Key => {
                self.ready_batch(device, volume, directory, scan, (key, false)).await?;
                answer_key_from_batch(scan, key)
            },
            ScanWant::Count | ScanWant::Locate | ScanWant::Bytes => None,
        };
        if let Some(step) = answer {
            return Ok(step);
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
                    scan.corrupt_steps += u64::from(step == ScanStep::Corrupt);
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
        // a sorted column with no mark, read at a point no sealed row hides, takes its sealed keys from the index and seats no
        // segment. A count takes whole blocks by their lines, so it keeps to the segments. A walk that changes path between steps
        // starts its places over, as one that meets a new list does
        // the drop count goes before the mark: a whole drop that lands between the two moves the count after it was read, so the
        // check after the batch read sees it
        let drops = self.sorted_index(scan.column).map_or(0, SortedKeys::drops_seen);
        volume.between_path_reads();
        let index = self.sorted_index(scan.column)
            .filter(|index| !scan.is_counting && index.mark().is_none() && !volume.may_hide_under_sealed(point));
        let is_path_changed = index.is_some() != scan.is_on_index || (index.is_some() && drops != scan.sorted_drops);
        scan.is_on_index = index.is_some();
        scan.sorted_drops = drops;
        if generation != scan.generation || is_path_changed {
            // a block taken by its line has no key to seek from, so a count that took one starts over and steps every row
            if generation != scan.generation && scan.taken > 0 {
                scan.restart_count();
            }
            scan.reseat(generation);
        }
        self.place_heads(volume, scan);
        match (index, directory) {
            (Some(index), _directory) => {
                fill_sorted(index, scan, &mut None);
                // a drop moves its count before it empties a part, so a batch read beside one sees the count here and seeks again
                if index.drops_seen() != scan.sorted_drops || index.mark().is_some() {
                    return Ok(StepOutcome::Moved);
                }
            },
            (None, Some(directory)) => self.join_segments(device, volume, directory, scan, point).await?,
            (None, None) => {},
        }

        let mut candidate = mem::take(&mut scan.key);
        candidate.clear();
        let mut has_key = scan.closest_key().is_some_and(|closest| {
            candidate.extend_from_slice(closest);
            true
        });
        // the index's next key stands beside the heads', and the nearer one goes first
        if let Some(sorted) = scan.sorted_key().filter(|sorted| !has_key || scan.is_before(sorted, &candidate)) {
            candidate.clear();
            candidate.extend_from_slice(sorted);
            has_key = true;
        }
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

    /// Take a key's newest sealed row from the index batch, when the batch stands on the key. An entry newer than the read point,
    /// or a value whose segment left when the step reads the value itself, sends the key to the footers. A located batch meets a
    /// segment that left at its own read, and sends that key back through a step that reads it
    pub async fn sorted_located(
        &self,
        device: &impl DeviceReads<Backend::File>,
        volume: &Volume<Backend>,
        scan: &mut ScanState,
        (candidate, is_reading): (&[u8], bool),
        read: &KeyRead<'_>,
    ) -> Result<Located, StoreError> {
        if scan.sorted_key() != Some(candidate) {
            return Ok(Located::Absent);
        }
        let entry = scan.sorted.entries[scan.sorted_next];
        let located = self.located_entry(&entry);
        let is_gone = is_reading && !entry.is_grave && matches!(located, Located::Sealed(_)) && !volume.table.is_readable(entry.segment);
        if entry.place > read.read_point || is_gone {
            self.sorted_footer_asks.fetch_add(1, Ordering::Relaxed);
            // an entry newer than the point can stand over an older row still open, which the write index holds at the point
            let open = self.index.lookup(scan.column, candidate, read.read_point, volume.view()).row;
            let sealed = self.search_sealed(device, volume, read).await?;
            return Ok(newest_of(open, sealed));
        }
        scan.sorted_scan_keys += 1;
        Ok(located)
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
        let other = if scan.is_on_index {
            self.sorted_located(device, volume, scan, (candidate, matches!(want, ScanWant::Value(_))), &read).await?
        } else {
            let taken = self.take_rows(device, volume, scan, candidate, read.read_point).await?;
            let sealed = if scan.is_spilled { self.search_sealed(device, volume, &read).await? } else { taken };
            sealed.map_or(Located::Absent, Located::Sealed)
        };
        // a seal moves rows from the index to a segment this walk hasn't seated, and both of its looks must see one list
        let is_same_list = at.directory.is_none_or(|directory| directory.generation() == at.generation);
        if !is_same_list {
            return Ok(StepOutcome::Moved);
        }

        let located = newest_located(found, other);
        let is_hidden = located.place().is_some_and(|place| read.cover.is_some_and(|cover| cover > place));
        if !located.has_value() || is_hidden {
            // a head shows the key and a live batch's point sees no row of it, so its put is still in flight
            if is_pinned && scan.point.is_none() && found.is_none() && !located.has_value() && scan.is_head_key(candidate) {
                scan.note_newer_passed(candidate);
            }
            scan.pass(candidate);
            return Ok(StepOutcome::Passed);
        }

        let answer = match want {
            ScanWant::Value(value) => self.read_located(on, &read, &located, &mut scan.window, scan.direction, value).await,
            ScanWant::Key | ScanWant::Count => {
                scan.counted += 1;
                Ok(true)
            },
            ScanWant::Locate => {
                // a row the walk's index entry names lends the value its leaf held, as a run's rows do, copied once when answered
                if is_held_for(scan, candidate, &located) {
                    let entry = BatchEntry { key_at: scan.sorted_next, key_length: candidate.len(), key_from: KeyFrom::Sorted(scan.sorted_turn), located, record: None, is_value: true, is_lent: true, is_trusted: false };
                    scan.batch.entries.push(entry);
                    scan.held_values += 1;
                } else {
                    let key_at = scan.batch.keys.len();
                    scan.batch.keys.extend_from_slice(candidate);
                    scan.batch.entries.push(BatchEntry { key_at, key_length: candidate.len(), key_from: KeyFrom::Batch, located, record: None, is_value: false, is_lent: false, is_trusted: false });
                }
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

    /// Locate the walk's next keys at one point with no value read, then read their small sealed records in joined runs when the
    /// walk wants values
    pub async fn fill_batch(
        &self,
        device: &impl DeviceReads<Backend::File>,
        volume: &Volume<Backend>,
        directory: Option<&ColumnDirectory>,
        scan: &mut ScanState,
        (key, is_reading): (&mut Vec<u8>, bool),
    ) -> Result<(), StoreError> {
        scan.batch.clear();
        // a pass a pinned single step noted belongs to no fill, so each fill notes its own
        scan.newer_passed_at = None;
        scan.batch.is_reading = is_reading;
        // the counts come before the batch locates a row, as a get takes them before its point. A read-only store checks every record
        scan.batch.counts = (!self.is_read_only).then(|| self.cues.counts());
        let point = scan.point.unwrap_or_else(|| self.read_point(volume));
        scan.batch.point = point;
        // each head stands at the batch's point: a shard whose count read after the point hasn't moved keeps its head
        if scan.is_on_index && point > scan.heads_mark {
            for (shard, head) in scan.heads.iter_mut().enumerate() {
                if head.is_placed && head.changes == self.index.change_count(shard) {
                    head.placed_at = head.placed_at.max(point);
                    continue;
                }
                head.cursor.reset();
                head.ahead.clear();
                head.is_placed = false;
            }
            scan.mark_heads();
        }
        let mut outcome = Ok(());
        // a batch takes keys while the records it reads still fit its buffer, so wide records read in batches of fewer keys
        let mut located_bytes = 0;
        // a caller that said how many rows it wants gets no more located than that, the one it asked for now among them
        let room = batch_room(scan.wanted, scan.asked, scan.untold_room);
        while scan.batch.entries.len() < room && scan.batch.has_room(located_bytes) {
            let entries = scan.batch.entries.len();
            match self.counted(self.try_step(ReadOn::new(device, volume), directory, scan, Some(point), key, ScanWant::Locate).await) {
                Ok(StepOutcome::Answer(ScanStep::Found) | StepOutcome::Passed) => {
                    // open rows read with the batch as sealed ones do, so both count toward its buffer
                    if let Some(entry) = scan.batch.entries.get(entries)
                        && entry.located.length().0 as usize <= MOST_BATCH_RECORD
                    {
                        located_bytes += entry.located.length().0 as usize;
                    }
                    self.take_sorted_run(volume, directory, scan, (point, room), &mut located_bytes);
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
        scan.end_batch();
        // an untold walk's next fill takes as many keys as it located so far, so it never reads more than twice what it took
        scan.untold_located += scan.batch.entries.len();
        scan.untold_room = scan.untold_located.clamp(FIRST_UNTOLD_KEYS, SCAN_BATCH_KEYS);
        // keys located before a failure answer first, and the next fill meets the failure again
        if scan.batch.entries.is_empty() {
            outcome?;
        }
        if is_reading {
            self.read_batch(device, volume, scan).await;
        }
        Ok(())
    }

    /// Locate the walk's next index and head keys in one loop while no cover, damage, new list or newer entry needs a step
    pub fn take_sorted_run(
        &self,
        volume: &Volume<Backend>,
        directory: Option<&ColumnDirectory>,
        scan: &mut ScanState,
        (point, room): (WritePlace, usize),
        located_bytes: &mut usize,
    ) {
        let Some(index) = self.sorted_index(scan.column).filter(|_index| scan.is_on_index) else {
            return;
        };
        if !volume.covers.is_empty() || !volume.damage.is_empty() {
            return;
        }
        // the step before the run passed a key, and a head that showed it moves on before the run reads the heads
        self.place_heads(volume, scan);
        let mut taken = 0;
        // the index key the run passed last, which the walk's position takes before a read of the index or a head placing
        let mut passed: Option<usize> = None;
        while scan.batch.entries.len() < room && scan.batch.has_room(*located_bytes) {
            // a row that landed since the heads were placed stands on no head, so a run needs heads placed at or above its point
            if directory.map_or(0, ColumnDirectory::generation) != scan.generation || point > scan.heads_mark {
                break;
            }
            if scan.sorted_next >= scan.sorted.len()
                && let Some(at) = passed.take()
            {
                scan.pass_sorted(at);
            }
            fill_sorted(index, scan, &mut passed);
            // a drop moves its count before it empties a part, and the next step seeks again
            if index.drops_seen() != scan.sorted_drops || index.mark().is_some() {
                break;
            }
            let sorted = scan.sorted_key();
            let head = scan.closest_head.map(|head| scan.heads[head].key.as_slice());
            // the nearer of the index's next key and the heads' goes first, and a key both show steps both
            let (next, is_head, is_sorted) = match (sorted, head) {
                (None, None) => break,
                (Some(sorted), None) => (sorted, false, true),
                (None, Some(head)) => (head, true, false),
                (Some(sorted), Some(head)) if sorted == head => (sorted, true, true),
                (Some(sorted), Some(head)) if scan.is_before(sorted, head) => (sorted, false, true),
                (Some(_sorted), Some(head)) => (head, true, false),
            };
            // a head standing on a bound has no key of its own there, so the step that answers one key at a time takes it from here
            if is_head && scan.closest_head.is_some_and(|head| scan.heads[head].is_bound) {
                break;
            }
            if scan.is_past_limit(next) {
                break;
            }
            let length = next.len();
            let sealed = match is_sorted.then(|| scan.sorted.entries[scan.sorted_next]) {
                // an entry newer than the point asks the footers, in the step that answers one key at a time
                Some(entry) if entry.place > point => break,
                Some(entry) => Some(self.located_entry(&entry)),
                None => None,
            };
            // no head shows an index key here, so its entry keeps the key where the index batch holds it
            let Some(shown) = scan.closest_head.filter(|_head| is_head) else {
                let located = sealed.unwrap_or(Located::Absent);
                let at = scan.sorted_next;
                if located.has_value() {
                    let from = KeyFrom::Sorted(scan.sorted_turn);
                    scan.batch.entries.push(BatchEntry { key_at: at, key_length: length, key_from: from, located, record: None, is_value: false, is_lent: false, is_trusted: false });
                    if located.length().0 as usize <= MOST_BATCH_RECORD {
                        *located_bytes += located.length().0 as usize;
                    }
                    taken += 1;
                }
                scan.sorted_next += 1;
                passed = Some(at);
                continue;
            };
            // the key goes into the batch once, and the batch lends it to the looks below while the scan is borrowed
            let key_at = scan.batch.keys.len();
            let ScanState { batch, heads, .. } = &mut *scan;
            batch.keys.extend_from_slice(&heads[shown].key);
            let mut keys = mem::take(&mut scan.batch.keys);
            let name = &keys[key_at..];
            volume.between_run_looks();
            let open = self.ahead_row(volume, scan, shown, name, point);
            // a seal between the two looks moves the row out of the view, so the step that answers one key at a time takes the key
            let is_moved = directory.map_or(0, ColumnDirectory::generation) != scan.generation;
            let located = open.filter(|_open| !is_moved).map(|open| newest_located(open, sealed.unwrap_or(Located::Absent)));
            if located.is_some() {
                // the head shows the key and a live run's point sees no row of it, so its put is still in flight
                if scan.point.is_none() && matches!(open, Some(None)) && located.is_some_and(|located| !located.has_value()) {
                    scan.note_newer_passed(name);
                }
                scan.pass(name);
            }
            if !located.is_some_and(|located| located.has_value()) {
                keys.truncate(key_at);
            }
            scan.batch.keys = keys;
            // the shard changed since its head read ahead, so the head descends fresh from the walk's place
            if open.is_none() {
                self.ahead_drops.fetch_add(1, Ordering::Relaxed);
                if let Some(at) = passed.take() {
                    scan.pass_sorted(at);
                }
                scan.heads[shown].forget_ahead();
                scan.heads[shown].is_placed = false;
                self.place_heads(volume, scan);
                continue;
            }
            let Some(located) = located else {
                break;
            };
            if located.has_value() {
                scan.batch.entries.push(BatchEntry { key_at, key_length: length, key_from: KeyFrom::Batch, located, record: None, is_value: false, is_lent: false, is_trusted: false });
                if located.length().0 as usize <= MOST_BATCH_RECORD {
                    *located_bytes += located.length().0 as usize;
                }
                taken += u64::from(matches!(located, Located::Sealed(_)));
            }
            passed = None;
            // a head with keys read ahead moves to the next one, and only a head that ran out places again
            if !scan.step_ahead(shown) {
                self.place_heads(volume, scan);
            }
        }
        if let Some(at) = passed {
            scan.pass_sorted(at);
        }
        scan.sorted_scan_keys += taken;
        if taken > 0 {
            self.sorted_runs.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Give the row of the key a head shows from the keys it read ahead, or read that key and the next few with their rows under
    /// one hold of the shard's lock when it has none. Nothing when the shard changed since they were read
    pub fn ahead_row(&self, volume: &Volume<Backend>, scan: &mut ScanState, shard: usize, name: &[u8], point: WritePlace) -> Option<Option<WriteRow>> {
        let (column, direction, view) = (scan.column, scan.direction, volume.view());
        let head = &mut scan.heads[shard];
        if head.ahead.count > 0 {
            let ahead = &mut head.ahead;
            let is_same_keys = ahead.next < ahead.count && ahead.changes == self.index.change_count(shard) && ahead.view == view;
            // a later batch's point keeps the keys of a shard that hasn't changed, and reads their rows again at the new point
            if is_same_keys && ahead.point != point && ahead.key(ahead.next) == name {
                let guard = self.index.shards.read(shard);
                if self.index.change_count(shard) == ahead.changes {
                    for index in ahead.next..ahead.count {
                        let next = ahead.key(index);
                        ahead.rows[index] = guard.lookup(hash_key(column, next), column, next, point, view).row;
                    }
                    ahead.point = point;
                    scan.lookups += (ahead.count - ahead.next) as u64;
                }
            }
            return scan.heads[shard].ahead.row_for(name, (self.index.change_count(shard), point, view));
        }
        let guard = self.index.shards.read(shard);
        // the count is read under the lock, so it matches the rows read
        let changes = self.index.change_count(shard);
        // a cursor that stood through a change can miss a row the change landed before it, so it descends fresh
        if head.changes != changes {
            head.cursor.reset();
        }
        let ahead = &mut head.ahead;
        (ahead.changes, ahead.point, ahead.view) = (changes, point, view);
        let mut has_room = ahead.push(name, guard.lookup(hash_key(column, name), column, name, point, view).row);
        let mut last: Option<EntryAddress> = None;
        while has_room {
            let bound = WalkBound { column, key: last.map_or(name, |entry| guard.slab.key(entry)), is_inclusive: false, direction };
            let Some(entry) = guard.next_entry(&mut head.cursor, &bound, view) else {
                ahead.is_cursor_past = true;
                break;
            };
            let next = guard.slab.key(entry);
            has_room = ahead.push(next, guard.lookup_from(entry, (column, next), point, view).row);
            last = Some(entry);
        }
        drop(guard);
        head.changes = changes;
        scan.lookups += head.ahead.count as u64;
        Some(head.ahead.rows[0])
    }

    /// Say whether the record cache can hold most of a volume's live records. Scans read values from it and keep what they
    /// read only then: at a smaller cache a lookup a key mostly misses, and what scans keep would push out what gets need
    pub fn scans_use_cache(&self, volume: &Volume<Backend>) -> bool {
        if !volume.is_scan_cache_weighed.load(Ordering::Relaxed) {
            self.weigh_scan_cache(volume);
        }
        volume.scans_use_cache.load(Ordering::Relaxed)
    }

    /// Weigh again whether the record cache can hold most of a volume's live records
    pub fn weigh_scan_cache(&self, volume: &Volume<Backend>) {
        let live = hold(&volume.summary).orders().live_bytes();
        let room = self.record_cache.share_bytes().0;
        let is_most = live > 0 && room.saturating_mul(SCAN_CACHE_PARTS) >= live.saturating_mul(SCAN_CACHE_PARTS - 1);
        volume.scans_use_cache.store(is_most, Ordering::Relaxed);
        volume.is_scan_cache_weighed.store(true, Ordering::Relaxed);
    }

    /// Read the batch's records together, from the record cache, a slot's window or joined reads, and leave the rest to own reads
    pub async fn read_batch(&self, device: &impl DeviceReads<Backend::File>, volume: &Volume<Backend>, scan: &mut ScanState) {
        // while the volume has damage marked the cache could hold bytes the scrub found bad, so the batch reads past it
        let is_cached = volume.damage.is_empty() && self.scans_use_cache(volume);
        let ScanState { batch: ScanBatch { entries, keys, records, rows, spans, trusted, words, counts, .. }, sorted, sorted_spare, sorted_turn, column, .. } = scan;
        rows.clear();
        rows.resize(entries.len(), None);
        let mut hits = 0;
        let mut held_rows = 0;
        let mut window_records = 0;
        for (row, entry) in rows.iter_mut().zip(entries.iter_mut()) {
            // a row its index leaf already answered takes no read
            if entry.is_value {
                continue;
            }
            // a value the index entry's leaf held came with the entry, open or sealed, so the row takes no read and no check
            if !matches!(entry.located, Located::Absent) && held_in((sorted, sorted_spare, *sorted_turn), entry).is_some() {
                entry.is_value = true;
                entry.is_lent = true;
                held_rows += 1;
                continue;
            }
            let sealed = match entry.located {
                Located::Sealed(sealed) => sealed,
                Located::Open(open) => {
                    entry.record = self.window_record(&open, records);
                    window_records += u64::from(entry.record.is_some());
                    if entry.record.is_none() {
                        *row = Some(BatchRow::open(&open));
                    }
                    continue;
                },
                Located::Absent => continue,
            };
            // a value is no longer than its record, so a record's room in the buffer holds it with no allocation
            let at = records.len();
            let is_room = at + sealed.row.length.0 as usize <= records.capacity();
            let key = key_in(keys, (sorted, sorted_spare, *sorted_turn), entry);
            if is_cached && is_room && self.record_cache.get_onto(sealed.segment, sealed.row.offset, key, records) == CacheLookup::Hit {
                entry.record = Some((at, records.len() - at));
                entry.is_value = true;
                hits += 1;
                continue;
            }
            *row = Some(BatchRow::sealed(&sealed));
        }
        if hits > 0 {
            self.scan_cache_hits.fetch_add(hits, Ordering::Relaxed);
        }
        if held_rows > 0 {
            self.scan_held_values.fetch_add(held_rows, Ordering::Relaxed);
        }
        if window_records > 0 {
            self.scan_window_records.fetch_add(window_records, Ordering::Relaxed);
        }
        spans.clear();
        spans.resize(entries.len(), None);
        trusted.clear();
        trusted.resize(entries.len(), false);
        if self.read_records(device, volume, rows, records, (spans, trusted), words).await {
            self.scan_batches_presorted.fetch_add(1, Ordering::Relaxed);
        }
        // a sealed record read after the batch's counts, with no reclaim or free since, is the record the index named, so it is
        // trusted as a get trusts one under its counts
        // a volume with damage marked checks every record, as a get's trusted fallback does
        let is_sealed_trusted = volume.damage.is_empty() && counts.is_some_and(|counts| self.has_outlived(counts, true));
        for ((entry, span), is_trusted) in entries.iter_mut().zip(spans.iter()).zip(trusted.iter()) {
            if span.is_some() {
                entry.record = *span;
                entry.is_trusted = match entry.located {
                    Located::Open(_) => *is_trusted,
                    Located::Sealed(_) => is_sealed_trusted,
                    Located::Absent => false,
                };
            }
        }
        // a column whose leaves hold values takes the records it read here, open and sealed, and keeps them a leaf at a time. A record
        // read under trust goes into its leaf unchecked, so rotten bytes read then are served from the leaf with no file read until
        // the leaf lets them go or the scrub marks the range. An open row's value stays right after its seal, since a seal moves no record
        let Some(index) = self.sorted_index(*column).filter(|index| index.values.page_count() > 0) else {
            return;
        };
        let mut is_fresh = [false; SCAN_BATCH_KEYS];
        let mut restored_values = 0;
        let mut trusted_records = 0;
        for (at, entry) in entries.iter_mut().enumerate() {
            let (Some((start, length)), Some(row), false) = (entry.record, held_row(&entry.located), entry.is_value) else {
                continue;
            };
            let record = &records[start..start + length];
            let key = key_in(keys, (sorted, sorted_spare, *sorted_turn), entry);
            let identity = RecordIdentity { record_key: self.record_key, column: *column, key, kind: RowKind::Value, place: row.0 };
            let Some(form) = form_of(record).filter(|form| !form.is_chunked) else {
                continue;
            };
            // an open record this run wrote, or a sealed one read under the batch's counts, is trusted as a get trusts it
            let is_trusted = entry.is_trusted;
            let payload = if is_trusted { trusted_payload(record, !matches!(entry.located, Located::Open(_))) } else { checked_payload(record, form, &identity) };
            let Some(payload) = payload.map(|payload| payload.len()) else {
                continue;
            };
            trusted_records += u64::from(is_trusted);
            let payload_at = start + length - payload;
            entry.record = match form.codec {
                Codec::None => Some((payload_at, payload)),
                // a compressed value is restored once into the buffer's spare room, so its leaf holds the value a get would read
                Codec::Lz4 => {
                    let at = records.len();
                    if at + MOST_HELD_VALUE > records.capacity() {
                        continue;
                    }
                    grow_into_room(records, at + MOST_HELD_VALUE);
                    let (read, room) = records.split_at_mut(at);
                    let Some(restored) = decompress(&read[payload_at..payload_at + payload], room) else {
                        records.truncate(at);
                        continue;
                    };
                    records.truncate(at + restored);
                    restored_values += 1;
                    Some((at, restored))
                },
            };
            entry.is_value = true;
            if let Some(fresh) = is_fresh.get_mut(at) {
                *fresh = true;
            }
        }
        if restored_values > 0 {
            self.scan_restored_values.fetch_add(restored_values, Ordering::Relaxed);
        }
        if trusted_records > 0 {
            self.scan_trusted_records.fetch_add(trusted_records, Ordering::Relaxed);
        }
        let fresh = entries.iter().enumerate().filter(|(at, _entry)| is_fresh.get(*at).copied().unwrap_or(false));
        index.hold_values(fresh.filter_map(|(_at, entry)| {
            let (Some(row), Some((start, length))) = (held_row(&entry.located), entry.record) else {
                return None;
            };
            let key = key_in(keys, (sorted, sorted_spare, *sorted_turn), entry);
            Some((key, row, &records[start..start + length]))
        }));
    }

    /// Copy an open record its slot's window still holds into the batch's buffer, with the slot held on the record's segment
    pub fn window_record(&self, open: &WriteRow, records: &mut Vec<u8>) -> Option<(usize, usize)> {
        let (at, length) = (records.len(), open.length.0 as usize);
        if self.is_read_only || length > MOST_WINDOW_RECORD_BYTES || at + length > records.capacity() {
            return None;
        }
        let _pin = self.slot_files[open.slot.0 as usize].pin(open.segment)?;
        grow_into_room(records, at + length);
        if self.windows[open.slot.0 as usize].iter().any(|window| window.read(open.offset, &mut records[at..])) {
            return Some((at, length));
        }
        records.truncate(at);
        None
    }

    /// Read small records in file and offset order, one read for each run of neighbours in one segment, sealed or open. Records
    /// already in that order skip the sort, and the answer says whether two or more did
    pub async fn read_records(
        &self,
        device: &impl DeviceReads<Backend::File>,
        volume: &Volume<Backend>,
        rows: &[Option<BatchRow>],
        records: &mut Vec<u8>,
        (spans, trusted): RecordSpans<'_>,
        words: &mut [u128],
    ) -> bool {
        let small = |index: u16| rows[index as usize].filter(|row| row.length.0 as usize <= MOST_BATCH_RECORD);
        let mut count = 0;
        for index in 0..rows.len().min(words.len()) as u16 {
            if let Some(row) = small(index) {
                words[count] = row.word(index);
                count += 1;
            }
        }
        let order = &mut words[..count];
        // a run reads from its first record's offset, so records taken as ordered are checked by the word the sort uses
        let is_presorted = order.is_sorted();
        if !is_presorted {
            order.sort_unstable();
        }
        let order = &*order;
        let small = |word: u128| small(index_of_word(word));
        let mut at = 0;
        let mut is_full = false;
        while at < order.len() && !is_full {
            // each run: its first and last place in the order, its file offset and its length
            let mut runs = [(0_usize, 0_usize, 0_u64, 0_u64); MOST_BATCH_RUNS];
            let mut sealed_pins: [Option<SegmentPin<'_, Backend::File>>; MOST_BATCH_RUNS] = std::array::from_fn(|_| None);
            let mut slot_pins: [Option<SlotPin<'_, Backend::File>>; MOST_BATCH_RUNS] = std::array::from_fn(|_| None);
            // the run whose pin each run reads under, and the file each run reads
            let mut pin_of = [0_usize; MOST_BATCH_RUNS];
            let mut runs_file = [(SegmentNumber(0), None); MOST_BATCH_RUNS];
            let (mut run_count, mut planned) = (0, 0_u64);
            let room = (records.capacity() - records.len()) as u64;
            while at < order.len() && run_count < MOST_BATCH_RUNS {
                let Some(first) = small(order[at]) else {
                    break;
                };
                let start = first.offset.0;
                let mut end = start + first.length.0;
                let mut last = at;
                let left = room - planned;
                while let Some(next) = order.get(last + 1).and_then(|word| small(*word)) {
                    let next_end = next.offset.0 + next.length.0;
                    let is_other_file = next.segment != first.segment || next.slot != first.slot;
                    if is_other_file || next.offset.0 > end + JOIN_GAP || next_end.max(end) - start > left {
                        break;
                    }
                    end = end.max(next_end);
                    last += 1;
                }
                if end - start > left {
                    is_full = true;
                    break;
                }
                // runs of one file sit side by side once sorted, so a run in the file the last run pinned reads under that pin
                let is_same_file = run_count > 0 && runs_file[run_count - 1] == (first.segment, first.slot);
                let is_pinned = if is_same_file {
                    pin_of[run_count] = pin_of[run_count - 1];
                    true
                } else {
                    pin_of[run_count] = run_count;
                    // an open segment's file is held through its slot, which lets go of a segment once it seals
                    match first.slot {
                        None => {
                            sealed_pins[run_count] = volume.table.pin(first.segment);
                            sealed_pins[run_count].is_some()
                        },
                        Some(slot) => {
                            slot_pins[run_count] = self.slot_files[slot.0 as usize].pin(first.segment);
                            slot_pins[run_count].is_some()
                        },
                    }
                };
                if is_pinned {
                    runs_file[run_count] = (first.segment, first.slot);
                    runs[run_count] = (at, last, start, end - start);
                    run_count += 1;
                    planned += end - start;
                }
                at = last + 1;
            }
            if run_count == 0 {
                break;
            }
            let base = records.len();
            // the room was written at open, so the reads land in it with no fill first
            grow_into_room(records, base + planned as usize);
            let mut reads: [Option<ManyRead<'_, Backend::File>>; MOST_BATCH_RUNS] = std::array::from_fn(|_| None);
            let mut rest = &mut records[base..];
            for (index, (read, &(_at, _last, start, length))) in reads.iter_mut().zip(&runs).enumerate().take(run_count) {
                let (buf, tail) = mem::take(&mut rest).split_at_mut(length as usize);
                rest = tail;
                let pin = pin_of[index];
                let file = sealed_pins[pin].as_ref().map(|pin| &pin.file).or_else(|| slot_pins[pin].as_ref().map(|pin| &pin.file));
                *read = file.map(|file| ManyRead { file, offset: FileOffset(start), buf, is_done: false });
            }
            device.read_many(&mut reads[..run_count]).await;
            let filled: [bool; MOST_BATCH_RUNS] = std::array::from_fn(|index| reads[index].as_ref().is_some_and(|read| read.is_done));
            let mut from = base;
            for (index, (&(first, last, start, length), is_filled)) in runs.iter().zip(filled).enumerate().take(run_count) {
                if is_filled {
                    // an open record past what recovery replayed was written by this run, as an open get trusts it
                    let recovered_end = slot_pins[pin_of[index]].as_ref().filter(|_pin| !self.is_read_only).map(|pin| pin.recovered_end);
                    for &word in &order[first..=last] {
                        if let Some(row) = small(word) {
                            let at = index_of_word(word) as usize;
                            spans[at] = Some((from + (row.offset.0 - start) as usize, row.length.0 as usize));
                            if let Some(is_trusted) = trusted.get_mut(at) {
                                *is_trusted = recovered_end.is_some_and(|end| row.offset >= end);
                            }
                        }
                    }
                }
                from += length as usize;
            }
        }
        is_presorted && count > 1
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
        let ScanState { batch, window, direction, column, sorted, sorted_spare, sorted_turn, .. } = scan;
        let Some(entry) = batch.entries.get(batch.next).copied() else {
            return Ok(None);
        };
        let name = key_in(&batch.keys, (sorted, sorted_spare, *sorted_turn), &entry);
        let lent = entry.is_lent.then(|| held_in((sorted, sorted_spare, *sorted_turn), &entry)).flatten();
        let held = match (entry.record, entry.located) {
            // a held value is copied once, from its page into the caller's buffer, and a leaf that let it go sends the row to its own read
            _lent if let Some(lent) = lent => {
                value.clear();
                let reading = self.value_reading(*column);
                self.sorted_index(*column).is_some_and(|index| index.read_held(lent, value, &reading))
            },
            // a value from the cache or a leaf's hold answers as it stands
            (Some((at, length)), Located::Sealed(_) | Located::Open(_)) if entry.is_value => {
                value.clear();
                value.extend_from_slice(&batch.records[at..at + length]);
                true
            },
            // a sealed row gets here only when its batch read was compressed or chunked, which a trusted get checks too
            (Some((at, length)), Located::Sealed(row)) => {
                let is_open = self.open_batched(on.device, (*column, name), (row.row.place, false), &batch.records[at..at + length], value);
                if is_open {
                    self.keep_scanned(on.target, (*column, name), &row, value);
                }
                is_open
            },
            // a record that fails its check here takes its own read, which says whether its slot moved on
            (Some((at, length)), Located::Open(row)) => self.open_batched(on.device, (*column, name), (row.place, entry.is_trusted), &batch.records[at..at + length], value),
            _other => false,
        };
        // only a row the batch doesn't hold needs the key's checks and cover for its own read
        let is_found = held || {
            let mut read = self.key_read(*column, name, batch.point, true)?;
            // a sealed record no leaf can hold goes straight to the caller, so it reads under the batch's counts as a get does. Only a
            // column whose leaves hold values reads it past the record cache, and a volume with damage marked checks it. A record
            // the batch read and found failing its check is checked again, so rot never passes as a value
            let is_passing = self.sorted_index(*column).is_some_and(|index| index.values.page_count() > 0) && on.target.damage.is_empty();
            let is_unread = entry.record.is_none();
            if is_passing && is_unread && batch.counts.is_some() && matches!(entry.located, Located::Sealed(_)) && entry.located.length().0 as usize > MOST_HELD_VALUE + SMALL_HEAD_BYTES {
                read.trust = batch.counts;
                self.scan_trusted_records.fetch_add(1, Ordering::Relaxed);
            }
            match self.read_located(on, &read, &entry.located, window, *direction, value).await {
                Ok(is_found) => is_found,
                Err(StoreError::Corruption { .. }) => false,
                Err(error) => return Err(error),
            }
        };
        if !is_found {
            scan.position.clear();
            scan.position.extend_from_slice(key_in(&scan.batch.keys, (&scan.sorted, &scan.sorted_spare, scan.sorted_turn), &entry));
            scan.is_inclusive = true;
            scan.generation = NO_GENERATION;
            scan.batch.clear();
            return Ok(None);
        }
        key.clear();
        key.extend_from_slice(scan.batch_key(&entry));
        scan.batch.next += 1;
        scan.batched += u64::from(held);
        Ok(Some(ScanStep::Found))
    }

    /// Take a guard on the sorted index's value room for one call into a scan, or one that counts nothing for a column with no index
    pub fn value_reading(&self, column: ColumnId) -> RoomRead<'_> {
        self.sorted_index(column).map_or_else(RoomRead::none, |index| index.values.room().reading())
    }

    /// Take the batch's next row when it can lend it: a value its sorted leaf holds, copied once into the value buffer, or a small
    /// plain record that checks out, given as its span in the batch. Nothing when the row needs the one-row step
    fn lend_next(&self, volume: &Volume<Backend>, scan: &mut ScanState, value: &mut Vec<u8>, reading: &RoomRead<'_>) -> Option<(BatchEntry, Option<(usize, usize)>)> {
        let entry = scan.batch.entries.get(scan.batch.next).copied()?;
        let held = entry.is_lent.then(|| held_in((&scan.sorted, &scan.sorted_spare, scan.sorted_turn), &entry)).flatten();
        let span = if held.is_some() { None } else { self.held_value(volume, scan, &entry) };
        if held.is_none() && span.is_none() {
            return None;
        }
        // a leaf that let its value go sends the row to the one-row step
        if let Some(held) = held {
            value.clear();
            if !self.sorted_index(scan.column).is_some_and(|index| index.read_held(held, value, reading)) {
                return None;
            }
        }
        scan.asked += 1;
        scan.batch.next += 1;
        scan.batched += 1;
        Some((entry, span))
    }

    /// Fill the buffers with the walk's next row, from the batch when it can lend the row, else through the one-row step
    pub fn next_scan_row(&self, scan: &mut ScanState, key: &mut Vec<u8>, value: &mut Vec<u8>) -> Result<ScanStep, StoreError> {
        let volume = &self.volumes[self.column(scan.column)?.volume.0 as usize];
        let reading = self.value_reading(scan.column);
        let Some((entry, span)) = self.lend_next(volume, scan, value, &reading) else {
            return self.step_scan(scan, key, ScanWant::Value(value));
        };
        key.clear();
        key.extend_from_slice(scan.batch_key(&entry));
        if let Some((at, length)) = span {
            value.clear();
            value.extend_from_slice(&scan.batch.records[at..at + length]);
        }
        Ok(ScanStep::Found)
    }

    /// Lend the walk's rows to a closure in order, each key and value as slices. A row whose small plain record the batch holds
    /// comes from where it lies once it checks out, and any other takes the one-row step. Ends after the row the closure says
    /// stop on, at the walk's end, or at a corrupt row
    pub fn drain_scan(
        &self,
        scan: &mut ScanState,
        (key, value): (&mut Vec<u8>, &mut Vec<u8>),
        mut each: impl FnMut(&[u8], &[u8]) -> bool,
    ) -> Result<ScanStep, StoreError> {
        let volume = &self.volumes[self.column(scan.column)?.volume.0 as usize];
        // one guard on the value room covers every row this call lends, and nothing holds it once the call returns. A closure
        // that parks inside it holds a release back for as long as it parks, and the extent waits for a later step
        let reading = self.value_reading(scan.column);
        loop {
            while let Some((entry, span)) = self.lend_next(volume, scan, value, &reading) {
                let row: &[u8] = match span {
                    Some((at, length)) => &scan.batch.records[at..at + length],
                    None => value,
                };
                if !each(scan.batch_key(&entry), row) {
                    return Ok(ScanStep::Found);
                }
            }
            match self.step_scan(scan, key, ScanWant::Value(value))? {
                ScanStep::Found if each(key, value) => {},
                step => return Ok(step),
            }
        }
    }

    /// Lend a key walk's keys to a closure in order from its batch, stepping for the next batch when one runs out. Ends after
    /// the key the closure says stop on, at the walk's end, or at a corrupt key
    pub fn drain_key_scan(&self, scan: &mut ScanState, key: &mut Vec<u8>, mut each: impl FnMut(&[u8]) -> bool) -> Result<ScanStep, StoreError> {
        loop {
            while let Some(entry) = scan.batch.entries.get(scan.batch.next).copied() {
                scan.batch.next += 1;
                scan.asked += 1;
                scan.batched += 1;
                if !each(scan.batch_key(&entry)) {
                    return Ok(ScanStep::Found);
                }
            }
            match self.step_scan(scan, key, ScanWant::Key)? {
                ScanStep::Found if each(key) => {},
                step => return Ok(step),
            }
        }
    }

    /// Find where a batch row's value lies in the batch's buffer: a value from the cache as it stands, or a small plain record's
    /// payload once the record checks out. Nothing for a row the one-row step answers
    pub fn held_value(&self, volume: &Volume<Backend>, scan: &ScanState, entry: &BatchEntry) -> Option<(usize, usize)> {
        let (at, length) = entry.record?;
        let place = match entry.located {
            Located::Sealed(_) | Located::Open(_) if entry.is_value => return Some((at, length)),
            Located::Sealed(row) => row.row.place,
            Located::Open(row) => row.place,
            Located::Absent => return None,
        };
        let (record, name) = (&scan.batch.records[at..at + length], scan.batch_key(entry));
        let identity = RecordIdentity { record_key: self.record_key, column: scan.column, key: name, kind: RowKind::Value, place };
        let form = form_of(record).filter(|form| !form.is_chunked && form.codec == Codec::None)?;
        let is_trusted = entry.is_trusted;
        let payload = if is_trusted { trusted_payload(record, !matches!(entry.located, Located::Open(_)))? } else { checked_payload(record, form, &identity)? };
        if is_trusted {
            self.scan_trusted_records.fetch_add(1, Ordering::Relaxed);
        }
        if let Located::Sealed(row) = entry.located {
            self.keep_scanned(volume, (scan.column, name), &row, payload);
        }
        Some((at + length - payload.len(), payload.len()))
    }

    /// Keep a value a scan read and checked: in its index leaf when the column's leaves hold values, else in the record cache
    /// while the cache can hold most of the volume, so gets keep theirs otherwise
    pub fn keep_scanned(&self, volume: &Volume<Backend>, (column, name): (ColumnId, &[u8]), row: &SealedRow, value: &[u8]) {
        // a column whose leaves hold values keeps them a batch at a time, in the batch's read
        if self.sorted_index(column).is_some_and(|index| index.values.page_count() > 0) {
            return;
        }
        // a value read while the volume has damage marked stays out, so the cache never takes bytes the scrub may have found bad
        if volume.scans_use_cache.load(Ordering::Relaxed) && volume.damage.is_empty() {
            self.record_cache.admit(row.segment, row.row.offset, name, value);
        }
    }

    /// Check and open a small record the batch read, as the window does
    pub fn open_batched(
        &self,
        device: &impl DeviceReads<Backend::File>,
        (column, key): (ColumnId, &[u8]),
        (place, is_trusted): (WritePlace, bool),
        record: &[u8],
        value: &mut Vec<u8>,
    ) -> bool {
        let identity = RecordIdentity {
            record_key: self.record_key,
            column,
            key,
            kind: RowKind::Value,
            place,
        };
        let Some(form) = form_of(record).filter(|form| !form.is_chunked) else {
            return false;
        };
        let opened = match form.codec {
            Codec::None if is_trusted => {
                let Some(payload) = trusted_payload(record, false) else {
                    return false;
                };
                self.scan_trusted_records.fetch_add(1, Ordering::Relaxed);
                value.clear();
                value.extend_from_slice(payload);
                RecordRead::Valid
            },
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
            Located::Open(row) => self.read_open(device, read, (row, volume.scans_use_cache.load(Ordering::Relaxed)), RecordWant::Whole, value).await,
            Located::Sealed(row) => {
                if let Some(answer) = self.read_from_window(on, read, row, window, direction, value).await {
                    return Ok(answer);
                }
                self.read_sealed(device, volume, read, (row, LeafAt::NONE), RecordWant::Whole, value).await
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
        // every place at or below the mark is in the index before the heads look
        let mark = self.read_point(volume);
        let view = volume.view();
        let is_new_view = view != scan.heads_view;
        scan.heads_view = view;
        let mut has_moved = false;
        // a volume with no row in the write index gives a head nothing to stand on, so the heads take no shard lock. A read-only
        // store keeps no such count, since its open rows come from a writer it follows
        if !self.is_read_only && volume.rows_in_index.load(Ordering::Acquire) == 0 {
            for head in scan.heads.iter_mut().filter(|head| !head.is_placed || is_new_view) {
                head.cursor.reset();
                head.ahead.clear();
                head.has_key = false;
                head.is_bound = false;
                head.is_placed = true;
                head.placed_at = mark;
                head.changes = NO_CHANGES;
            }
            scan.find_closest_head();
            return;
        }
        // a shard whose open counts show no open key near the walk takes no lock and no descent: its head stands on a bound the
        // walk places for real once it gets there. The count of changes is read first, so a write landing after it moves the count
        // Rows entered in a sorted index ahead of their seal left the counts, so only a walk that reads that index may skip them
        let may_bound = scan.is_on_index || self.sorted_index(scan.column).is_none();
        if !self.is_read_only && may_bound {
            let ScanState { heads, position, direction, column: column_of, .. } = &mut *scan;
            let mut skipped = 0;
            let width = self.column(*column_of).map_or(KeyShape::Variable { most: 0 }, |info| info.key_shape);
            for (shard, head) in heads.iter_mut().enumerate().filter(|(_shard, head)| !head.is_placed || is_new_view) {
                let changes = self.index.change_count(shard);
                // a shard with no open row of the column its index lacks stands its head past the end, whatever the keys look like
                let counts = self.index.shard_counts(shard);
                let open = match counts.is_column_quiet(*column_of) {
                    true => Some(OpenBound::End),
                    false => counts.of(*column_of).and_then(|counts| counts.bound(position, *direction)),
                };
                let Some(open) = open else {
                    continue;
                };
                head.cursor.reset();
                head.ahead.clear();
                head.key.clear();
                head.has_key = matches!(open, OpenBound::Key(_));
                head.is_bound = head.has_key;
                if let OpenBound::Key(bytes) = open {
                    match width {
                        // every key of a fixed width sorts at or past the bound padded with zeros to that width
                        KeyShape::Fixed { width } => {
                            let width = width as usize;
                            head.key.extend_from_slice(&bytes[..width.min(bytes.len())]);
                            head.key.resize(width, 0);
                        },
                        // a key shorter than the bound's eight bytes still sorts at or past its trimmed form
                        KeyShape::Variable { .. } => {
                            let length = bytes.iter().rposition(|byte| *byte != 0).map_or(1, |last| last + 1);
                            head.key.extend_from_slice(&bytes[..length]);
                        },
                    }
                }
                head.is_placed = true;
                head.placed_at = mark;
                head.changes = changes;
                has_moved = true;
                skipped += 1;
            }
            if skipped > 0 {
                self.heads_skipped.fetch_add(skipped, Ordering::Relaxed);
            }
        }
        let ScanState { heads, position, is_inclusive, direction, column, .. } = &mut *scan;
        let bound = WalkBound { column: *column, key: position, is_inclusive: *is_inclusive, direction: *direction };
        let mut wanted = heads.iter_mut().enumerate().filter(|(_shard, head)| !head.is_placed || is_new_view).peekable();
        // a few shards a batch, so their tree descents overlap and writers wait on no more than those shards' locks
        while wanted.peek().is_some() {
            let mut group: [Option<(usize, &mut ShardHead)>; HEAD_GROUP] = [const { None }; HEAD_GROUP];
            for member in &mut group {
                *member = wanted.next();
            }
            // the read locks go in shard order, as the shards came
            let guards: [Option<RwLockReadGuard<'_, WriteShard>>; HEAD_GROUP] =
                std::array::from_fn(|index| group[index].as_ref().map(|(shard, _head)| self.index.shards.read(*shard)));
            // a cursor that stood through a change can miss a row the change landed before it, so it descends fresh
            for (shard, head) in group.iter_mut().flatten() {
                head.forget_ahead();
                if !head.cursor.is_reset() && head.changes != self.index.change_count(*shard) {
                    head.cursor.reset();
                }
            }
            let mut places: [Option<HeadPlace<'_>>; HEAD_GROUP] = [const { None }; HEAD_GROUP];
            for ((member, guard), place) in group.iter_mut().zip(&guards).zip(&mut places) {
                if let (Some((_shard, head)), Some(guard)) = (member.as_mut(), guard.as_ref()) {
                    *place = Some(HeadPlace { shard: guard, cursor: &mut head.cursor, key: &mut head.key, has_key: &mut head.has_key });
                }
            }
            let descended = place_heads_together(&mut places, &bound, view);
            self.heads_together.fetch_add(descended as u64, Ordering::Relaxed);
            // each count is read under its shard's lock, so it matches the rows the head saw
            for (shard, head) in group.iter_mut().flatten() {
                head.changes = self.index.change_count(*shard);
            }
            drop(guards);
            for (_shard, head) in group.iter_mut().flatten() {
                head.is_placed = true;
                head.is_bound = false;
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

    /// Seat every segment a range of bounds meets, a batch at a time. What the sources can't hold spills, and goes by seeks from
    /// then on
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
        let mut batch = [SegmentNumber(0); SEAT_BATCH];
        loop {
            let page = directory.overlapping(low, high, skip, &mut scan.found);
            skip += scan.found.len();
            // the page's seats go in even batches, so a list one past a batch's room seats two halves and none alone
            let seats = scan.found.iter().filter(|segment| !scan.holds(**segment)).count();
            let batch_size = seats.div_ceil(seats.div_ceil(SEAT_BATCH).max(1)).max(1);
            let mut count = 0;
            for index in 0..scan.found.len() {
                let segment = scan.found[index];
                if scan.holds(segment) {
                    continue;
                }
                if scan.source_count + count == scan.sources.len() {
                    scan.is_spilled = true;
                    continue;
                }
                batch[count] = segment;
                count += 1;
                if count == batch_size {
                    self.seat_together(ReadOn::new(device, volume), scan, &batch[..count], point).await?;
                    count = 0;
                }
            }
            self.seat_together(ReadOn::new(device, volume), scan, &batch[..count], point).await?;
            if !page.is_cut {
                return Ok(());
            }
        }
    }

    /// Seat listed segments in the next free sources. The seats whose blocks the row cache holds search them together
    /// so their cache misses overlap, and every other seat goes alone as before. A segment gone from under the list is in the
    /// next one, which the step's last look will see
    pub async fn seat_together(
        &self,
        on: ReadOn<'_, impl DeviceReads<Backend::File>, Volume<Backend>>,
        scan: &mut ScanState,
        segments: &[SegmentNumber],
        point: WritePlace,
    ) -> Result<(), StoreError> {
        let ReadOn { device, target: volume } = on;
        let ScanState { column, position, is_inclusive, direction, is_counting, sources, source_count, is_tree_stale, touched_sources, .. } = scan;
        let at = SeatAt {
            column: *column,
            key: position,
            is_inclusive: *is_inclusive,
            direction: *direction,
            point,
            is_counting: *is_counting,
        };
        let count = segments.len().min(SEAT_BATCH).min(sources.len() - *source_count);
        *touched_sources = (*touched_sources).max(*source_count + count);
        let seats = &mut sources[*source_count..*source_count + count];
        let mut keys = [NO_BLOCK; SEAT_BATCH];
        for ((source, segment), key) in seats.iter_mut().zip(segments).zip(&mut keys) {
            source.segment = *segment;
            if let SeatPlace::Block { start, line, replacement } = self.seat_place(volume, source, &at)
                && (line.live_rows > 0 || replacement > at.point)
            {
                source.block = start;
                source.enter_block(line);
                *key = BlockKey { segment: *segment, offset: line.block_offset };
            }
        }
        let mut rows = [None; SEAT_BATCH];
        if count > 1 {
            self.row_cache.with_many(&keys[..count], |blocks| seek_together(seats, blocks, at.key, at.is_inclusive, at.direction, &mut rows));
        }
        let mut seated = 0;
        for index in 0..count {
            let step = match rows[index] {
                Some(row) => {
                    seats[index].row = row;
                    SourceMove::Row
                },
                None => self.seat_source(device, volume, &mut seats[index], &at).await?,
            };
            if step == SourceMove::Row {
                seats.swap(seated, index);
                seated += 1;
            }
        }
        *source_count += seated;
        if seated > 0 {
            *is_tree_stale = true;
        }
        let together = rows[..count].iter().flatten().count() as u64;
        self.scan_seats.fetch_add(count as u64, Ordering::Relaxed);
        if together > 0 {
            self.seats_together.fetch_add(together, Ordering::Relaxed);
        }
        Ok(())
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
        let ScanState { column, position, is_inclusive, direction, is_counting, sources, spill, source_count, touched_sources, .. } = scan;
        let at = SeatAt {
            column: *column,
            key: position,
            is_inclusive: *is_inclusive,
            direction: *direction,
            point,
            is_counting: *is_counting,
        };
        if !is_spill {
            *touched_sources = (*touched_sources).max(*source_count + 1);
        }
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
            if line.live_rows > 0 || replacement > point || !can_cross_unread(volume, source.segment) {
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
        let (start, line, replacement) = match self.seat_place(volume, source, at) {
            SeatPlace::Moved(step) => return Ok(step),
            SeatPlace::Block { start, line, replacement } => (start, line, replacement),
            // a key that matches more shared bytes than the tail holds takes the column's first row to place
            SeatPlace::Unsettled => {
                let tail = source.tail;
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
        if line.live_rows == 0 && replacement <= at.point && can_cross_unread(volume, source.segment) {
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

    /// Set a source up on its segment and find the block its seat starts in, with no read of the device
    pub fn seat_place(&self, volume: &Volume<Backend>, source: &mut SegmentSource, at: &SeatAt<'_>) -> SeatPlace {
        let seat = match volume.table.seat_line(source.segment, at.column, at.key, at.direction) {
            Seat::Left => return SeatPlace::Moved(SourceMove::Left),
            Seat::NoColumn => return SeatPlace::Moved(SourceMove::Done),
            Seat::Line(seat) => seat,
        };
        // a segment whose every row is newer than the walk's point holds nothing it shows, so it takes no source. A count
        // seats it all the same: its keys keep the count from taking an older block by its live rows, which its graves cut
        if !at.is_counting && seat.head.places.lowest > at.point {
            return SeatPlace::Moved(SourceMove::Done);
        }
        let tail = seat.tail;
        source.place_base = seat.head.place_base;
        let can_take = at.is_counting && seat.is_walked && seat.head.places.highest <= at.point;
        source.is_countable = can_take && has_no_live_graves(volume, source.segment, at.column) && volume.dead_bits.exact(source.segment).is_some();
        source.tail = tail;
        source.first_block = tail.first_block;
        source.end_block = tail.first_block + tail.block_count.0;
        match (seat.is_settled, seat.start) {
            (true, Some((start, line))) => SeatPlace::Block { start, line, replacement: seat.replacement },
            (true, None) => SeatPlace::Moved(SourceMove::Done),
            (false, _start) => SeatPlace::Unsettled,
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
        if let Some(count) = self.count_on_index(volume, column, prefix, point) {
            self.index_counts.fetch_add(1, Ordering::Relaxed);
            return Ok(RowCount(count));
        }
        let mut scan = self.take_scan(&under_prefix(column, prefix), Some(point))?;
        scan.is_counting = volume.covers.is_empty() && volume.damage.is_empty();
        let outcome = self.walk_prefix(&mut scan, ScanWantKind::Key);
        let count = RowCount(scan.counted);
        self.give_back_scan(scan);
        outcome.map(|()| count)
    }

    /// Count a prefix from its column's sorted index alone, when the index holds every row at the point: it has no mark, every
    /// shard holds no open row of the column it lacks, and no cover, damage or newer sealed row stands over the point. Nothing
    /// when it can't, and the walk counts
    pub fn count_on_index(&self, volume: &Volume<Backend>, column: ColumnId, prefix: &[u8], point: WritePlace) -> Option<u64> {
        let index = self.sorted_index(column)?;
        // the drop count goes first, so a drop and a refill that both pass during the count show
        let drops = index.drops_seen();
        if index.mark().is_some() {
            return None;
        }
        if !volume.covers.is_empty() || !volume.damage.is_empty() || volume.may_hide_under_sealed(point) {
            return None;
        }
        // rows at or below the point are in the write index already, so a quiet column read after the point holds them all
        if !(0..SHARD_COUNT).all(|shard| self.index.shard_counts(shard).is_column_quiet(column)) {
            return None;
        }
        let count = index.count_live(prefix, point)?;
        // a drop moves its count before it empties a part, and a drop and a refill can both pass inside one count
        if index.drops_seen() != drops || index.mark().is_some() {
            return None;
        }
        // a seal that started during the count can drop an entry under a grave, and it marks the volume before it does
        if !volume.covers.is_empty() || !volume.damage.is_empty() || volume.may_hide_under_sealed(point) {
            return None;
        }
        Some(count)
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
                ScanWantKind::Key => ScanWant::Count,
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
            ScanWant::Count => ScanWant::Count,
            ScanWant::Value(value) => ScanWant::Value(value),
            ScanWant::Bytes => ScanWant::Bytes,
            ScanWant::Locate => ScanWant::Locate,
        }
    }
}

/// Where each read record landed in the batch's buffer, and whether its reader trusts it
pub type RecordSpans<'spans> = (&'spans mut [Option<(usize, usize)>], &'spans mut [bool]);

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

/// Answer the batch's next key with no value read, as of the batch's point. Nothing when the batch is used up
pub fn answer_key_from_batch(scan: &mut ScanState, key: &mut Vec<u8>) -> Option<ScanStep> {
    let entry = scan.batch.entries.get(scan.batch.next).copied()?;
    key.clear();
    key.extend_from_slice(scan.batch_key(&entry));
    scan.batch.next += 1;
    Some(ScanStep::Found)
}

/// Bring a walk's batch of index entries to the first key it hasn't passed, reading the next batch from its position when the
/// batch runs out and the index holds more
pub fn fill_sorted(index: &SortedKeys, scan: &mut ScanState, passed: &mut Option<usize>) {
    if scan.sorted.width() != index.width() {
        scan.keep_sorted_keys(true);
        scan.sorted.set_width(index.width());
        scan.sorted_spare.set_width(index.width());
        scan.sorted_next = 0;
        scan.is_sorted_done = false;
    }
    // open rows entered since the batch was read may have left the open counts, so the batch is read again from the walk's
    // position. The count is read before the read, so a later entering shows as a change
    let entered = index.entered();
    if entered != scan.sorted_entered {
        // the batch read again starts from the walk's position, which first takes the key a run passed last
        if let Some(at) = passed.take() {
            scan.pass_sorted(at);
        }
        scan.forget_sorted();
    }
    loop {
        while let Some(key) = scan.sorted_key() {
            if scan.is_ahead(key) {
                return;
            }
            scan.sorted_next += 1;
        }
        if scan.is_sorted_done {
            return;
        }
        // the index hands out no more entries than the caller still wants, the one asked for now among them
        let most = batch_room(scan.wanted, scan.asked, scan.untold_room).max(1);
        // the read fills the spare, so the batch's entries keep their keys in the index batch it read last
        scan.turn_sorted();
        scan.sorted_entered = entered;
        // a walk for keys alone takes no held value
        scan.sorted.is_with_values = scan.batch.is_reading;
        let ScanState { sorted, position, is_inclusive, direction, .. } = scan;
        let from = ScanFrom::Key { key: position, is_inclusive: *is_inclusive };
        let is_done = match direction {
            Direction::Ascending => index.read_after(from, most, sorted),
            Direction::Descending => index.read_before(from, most, sorted),
        };
        scan.sorted_next = 0;
        scan.is_sorted_done = is_done;
        // a batch with graves in it fills before the entries a call looks at run out, so an empty batch has nothing left
        if scan.sorted.is_empty() {
            scan.is_sorted_done = true;
        }
    }
}

/// Check a block whose line counts no live row can be crossed unread. Only a segment with exact bits has a bit behind every
/// charge on its lines, so a block of any other segment is read
pub fn can_cross_unread<Backend: Io>(volume: &Volume<Backend>, segment: SegmentNumber) -> bool {
    let is_exact = volume.dead_bits.exact(segment).is_some();
    if !is_exact {
        volume.zero_lines_read.fetch_add(1, Ordering::Relaxed);
    }
    is_exact
}

/// Check the index batch holds the value of a located row, when the walk's index entry is the row's
fn is_held_for(scan: &ScanState, candidate: &[u8], located: &Located) -> bool {
    if !scan.is_on_index || scan.sorted_key() != Some(candidate) {
        return false;
    }
    let entry = scan.sorted.entries[scan.sorted_next];
    held_row(located) == Some((entry.place, entry.segment, entry.offset)) && scan.sorted.held_value(scan.sorted_next).is_some()
}

/// Read the place, segment and offset a leaf's held value answers for, for an open or a sealed row
pub fn held_row(located: &Located) -> Option<(WritePlace, SegmentNumber, FileOffset)> {
    match located {
        Located::Sealed(sealed) => Some((sealed.row.place, sealed.segment, sealed.row.offset)),
        Located::Open(open) => Some((open.place, open.segment, open.offset)),
        Located::Absent => None,
    }
}
