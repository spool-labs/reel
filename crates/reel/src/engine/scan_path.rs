//! Scans: merge the sealed segments and the write index in key order, and answer each key at the newest point or a cue's

use std::cmp::Ordering as KeyOrder;
use std::io::Error;
use std::mem;
use std::sync::atomic::Ordering;

use reel_api::column_id::ColumnId;
use reel_api::direction::Direction;
use reel_api::scan::ScanStep;
use reel_api::store_error::StoreError;
use reel_api::units::WritePlace;

use crate::engine::engine_state::EngineState;
use crate::engine::read_path::{KeyRead, Located, MOST_READ_TRIES, RecordWant, SealedRow, newest_of};
use crate::engine::scan_state::{MOST_SCANS, NO_GENERATION, ScanRange, ScanState, SegmentSource};
use crate::engine::volume::Volume;
use crate::guard::hold;
use crate::index::write_shard::WalkBound;
use crate::io::Io;
use crate::segment::block_cache::BlockKey;
use crate::segment::block_entry::{BlockEntry, KeyBound, SHARED_HEAD_BYTES};
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
        let Some(mut scan) = hold(&self.scans).pop() else {
            return Err(StoreError::ScansFull { limit: MOST_SCANS as u64 });
        };
        scan.start(range, point);
        Ok(scan)
    }

    /// Hand a scan's buffers back
    pub fn give_back_scan(&self, scan: ScanState) {
        hold(&self.scans).push(scan);
    }

    /// Take one step of a scan, with a value buffer when it wants values. A job that crosses a try costs a seek from the last
    /// key, and past the usual tries a pin holds the jobs off that key
    pub fn step_scan(
        &self,
        scan: &mut ScanState,
        key: &mut Vec<u8>,
        mut value: Option<&mut Vec<u8>>,
    ) -> Result<ScanStep, StoreError> {
        let info = self.column(scan.column)?;
        let volume = &self.volumes[info.volume.0 as usize];
        let directory = self.directories[scan.column.0 as usize].as_ref();
        let mut tries = 0;
        loop {
            if self.is_stopping.load(Ordering::Acquire) {
                return Err(StoreError::Closed);
            }
            let many = if tries >= MOST_READ_TRIES && scan.point.is_none() { self.many_read(volume) } else { None };
            let pinned = many.and_then(|many| many.pin.map(|slot| (many, slot)));
            let outcome = self.try_step(volume, directory, scan, pinned.map(|(many, _slot)| many.point), key, value.as_deref_mut());
            if let Some((many, slot)) = pinned {
                self.unpin(slot, many.counts);
            }
            match outcome? {
                StepOutcome::Answer(step) => return Ok(step),
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
    pub fn try_step(
        &self,
        volume: &Volume<Backend>,
        directory: Option<&ColumnDirectory>,
        scan: &mut ScanState,
        pin_point: Option<WritePlace>,
        key: &mut Vec<u8>,
        value: Option<&mut Vec<u8>>,
    ) -> Result<StepOutcome, StoreError> {
        let generation = directory.map_or(0, ColumnDirectory::generation);
        let fixed_point = scan.point.or(pin_point);
        let point = fixed_point.unwrap_or_else(|| self.read_point());
        if generation != scan.generation {
            scan.reseat(generation);
        }
        self.place_heads(volume, scan);
        if let Some(directory) = directory {
            self.join_segments(volume, directory, scan, point)?;
        }

        let mut candidate = mem::take(&mut scan.key);
        candidate.clear();
        let mut has_key = scan.closest_key().is_some_and(|closest| {
            candidate.extend_from_slice(closest);
            true
        });
        if let Some(directory) = directory.filter(|_directory| scan.is_spilled) {
            has_key = self.spilled_key(volume, directory, scan, point, has_key, &mut candidate)?;
        }
        has_key &= !scan.is_past_limit(&candidate);

        let outcome = match self.check_reach(volume, scan, has_key.then_some(&candidate)) {
            Ok(()) if has_key => {
                let at = StepAt { directory, generation, fixed_point };
                self.answer_key(volume, scan, &candidate, at, key, value)
            },
            Ok(()) => Ok(StepOutcome::Answer(ScanStep::End)),
            Err(error) => Err(error),
        };
        scan.key = candidate;
        outcome
    }

    /// Answer the walk's next key at a point
    pub fn answer_key(
        &self,
        volume: &Volume<Backend>,
        scan: &mut ScanState,
        candidate: &[u8],
        at: StepAt,
        key: &mut Vec<u8>,
        value: Option<&mut Vec<u8>>,
    ) -> Result<StepOutcome, StoreError> {
        // the counts come before the point, as a get takes them
        let counts = self.cues.counts();
        let read = match at.fixed_point {
            Some(point) => self.key_read(scan.column, candidate, point, true)?,
            None => self.key_read_now(scan.column, candidate)?,
        };
        let is_pinned = at.fixed_point.is_some();
        let found = self.index.lookup(scan.column, candidate, read.read_point, volume.view());
        let taken = self.take_rows(volume, scan, candidate, read.read_point)?;
        let sealed = if scan.is_spilled { self.search_footers(volume, &read)? } else { taken };
        // a seal moves rows from the index to a segment this walk hasn't seated, and both of its looks must see one list
        let is_same_list = at.directory.is_none_or(|directory| directory.generation() == at.generation);
        if !is_same_list {
            return Ok(StepOutcome::Moved);
        }

        let located = newest_of(found.row, sealed);
        let is_hidden = located.place().is_some_and(|place| read.cover.is_some_and(|cover| cover > place));
        if !located.has_value() || is_hidden {
            scan.pass(candidate);
            return Ok(StepOutcome::Passed);
        }

        let answer = match value {
            Some(value) => self.read_located(volume, &read, &located, value),
            None => Ok(true),
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

    /// Read the value of the row a key landed on, and return false when its segment or slot moved on
    pub fn read_located(
        &self,
        volume: &Volume<Backend>,
        read: &KeyRead,
        located: &Located,
        value: &mut Vec<u8>,
    ) -> Result<bool, StoreError> {
        match located {
            Located::Absent => Ok(false),
            Located::Open(row) => self.read_open(read, row, RecordWant::Whole, value),
            Located::Sealed(row) => self.read_sealed(volume, read, row, RecordWant::Whole, value),
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
        let view = volume.view();
        for (shard, head) in scan.heads.iter_mut().enumerate() {
            if !head.is_placed {
                head.has_key = self.index.next_key(shard, &mut head.cursor, &bound, view, &mut head.key);
                head.is_placed = true;
            }
        }
    }

    /// Seat every segment that starts before the walk's next key, one bound of the list at a time
    pub fn join_segments(
        &self,
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
            self.join_range(volume, directory, scan, &low, &high, point)?;
            scan.frontier = match scan.direction {
                Direction::Ascending => directory.next_above(&reach),
                Direction::Descending => directory.next_below(&reach),
            };
        }
        Ok(())
    }

    /// Seat every segment a range of bounds meets. What the sources can't hold spills, and goes by seeks from then on
    pub fn join_range(
        &self,
        volume: &Volume<Backend>,
        directory: &ColumnDirectory,
        scan: &mut ScanState,
        low: &KeyBound,
        high: &KeyBound,
        point: WritePlace,
    ) -> Result<(), StoreError> {
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
                let at = SeatAt {
                    column: scan.column,
                    key: &scan.position,
                    is_inclusive: scan.is_inclusive,
                    direction: scan.direction,
                    point,
                };
                let source = &mut scan.sources[scan.source_count];
                source.segment = segment;
                // a segment gone from under the list is in the next one, which the step's last look will see
                if self.seat_source(volume, source, &at)? == SourceMove::Row {
                    scan.source_count += 1;
                }
            }
            if !page.is_cut {
                return Ok(());
            }
        }
    }

    /// Find the walk's next key among the segments the sources couldn't hold, and say whether it has one
    pub fn spilled_key(
        &self,
        volume: &Volume<Backend>,
        directory: &ColumnDirectory,
        scan: &mut ScanState,
        point: WritePlace,
        has_key: bool,
        candidate: &mut Vec<u8>,
    ) -> Result<bool, StoreError> {
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
                let at = SeatAt {
                    column: scan.column,
                    key: &scan.position,
                    is_inclusive: scan.is_inclusive,
                    direction: scan.direction,
                    point,
                };
                scan.spill.segment = segment;
                if self.seat_source(volume, &mut scan.spill, &at)? == SourceMove::Row
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

    /// Walk every source past a key, and return its newest row at a point
    pub fn take_rows(
        &self,
        volume: &Volume<Backend>,
        scan: &mut ScanState,
        key: &[u8],
        point: WritePlace,
    ) -> Result<Option<SealedRow>, StoreError> {
        let mut best: Option<SealedRow> = None;
        let (column, direction) = (scan.column, scan.direction);
        let mut index = 0;
        while index < scan.source_count {
            let source = &mut scan.sources[index];
            if source.key != key {
                index += 1;
                continue;
            }
            let mut step = SourceMove::Row;
            while step == SourceMove::Row && source.key == key {
                if source.row.place <= point && best.is_none_or(|row| row.row.place < source.row.place) {
                    best = Some(SealedRow {
                        segment: source.segment,
                        row: source.row,
                        block_offset: source.line.block_offset,
                    });
                }
                step = self.step_source(volume, column, source, direction, point)?;
            }
            match step {
                SourceMove::Row => index += 1,
                // a segment leaves its list before the table, so the step's look at the list sees this
                SourceMove::Done | SourceMove::Left => scan.drop_source(index),
            }
        }
        Ok(best)
    }

    /// Move a source to its next row in the walk's direction
    pub fn step_source(
        &self,
        volume: &Volume<Backend>,
        column: ColumnId,
        source: &mut SegmentSource,
        direction: Direction,
        point: WritePlace,
    ) -> Result<SourceMove, StoreError> {
        loop {
            let (segment, line) = (source.segment, source.line);
            let row = self.visit_block(volume, column, segment, &line, |block| match direction {
                Direction::Ascending => source.next_in_block(block),
                Direction::Descending => source.previous_in_block(block),
            })?;
            match row {
                None => return Ok(SourceMove::Left),
                Some(Some(row)) => {
                    source.row = row;
                    return Ok(SourceMove::Row);
                },
                Some(None) => {
                    let crossed = self.cross_block(volume, source, direction, point);
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
            if line.live_rows > 0 || replacement > point {
                source.enter_block(line);
                return SourceMove::Row;
            }
        }
    }

    /// Stand a source on its segment's first row past a key
    pub fn seat_source(
        &self,
        volume: &Volume<Backend>,
        source: &mut SegmentSource,
        at: &SeatAt,
    ) -> Result<SourceMove, StoreError> {
        let Some(pin) = volume.table.pin(source.segment) else {
            return Ok(SourceMove::Left);
        };
        source.place_base = pin.head.place_base;
        drop(pin);
        let Some(tail) = volume.table.column_tail(source.segment, at.column) else {
            return Ok(SourceMove::Done);
        };
        source.first_block = tail.first_block;
        source.end_block = tail.first_block + tail.block_count.0;
        let spot = self.column_spot(volume, source, &tail, at)?;
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
        source.block = start;
        source.enter_block(line);
        if line.live_rows == 0 && replacement <= at.point {
            let crossed = self.cross_block(volume, source, at.direction, at.point);
            if crossed != SourceMove::Row {
                return Ok(crossed);
            }
        }
        loop {
            let (segment, line) = (source.segment, source.line);
            let row = self.visit_block(volume, at.column, segment, &line, |block| match at.direction {
                Direction::Ascending => source.seek_up(block, at.key, at.is_inclusive),
                Direction::Descending => source.seek_down(block, at.key, at.is_inclusive),
            })?;
            match row {
                None => return Ok(SourceMove::Left),
                Some(Some(row)) => {
                    source.row = row;
                    return Ok(SourceMove::Row);
                },
                Some(None) => {
                    let crossed = self.cross_block(volume, source, at.direction, at.point);
                    if crossed != SourceMove::Row {
                        return Ok(crossed);
                    }
                },
            }
        }
    }

    /// Place a walk's key against the bytes a column's keys share, which the block index leaves out. A prefix longer than the
    /// tail holds takes the column's first key to settle
    pub fn column_spot(
        &self,
        volume: &Volume<Backend>,
        source: &mut SegmentSource,
        tail: &ColumnTail,
        at: &SeatAt,
    ) -> Result<ColumnSpot, StoreError> {
        let shared = tail.shared_length as usize;
        let mut order = tail.order_against_shared(at.key);
        if order == KeyOrder::Equal && shared > SHARED_HEAD_BYTES {
            let Some((line, _replacement)) = volume.table.block_line(source.segment, tail.first_block) else {
                return Ok(ColumnSpot::Inside);
            };
            source.enter_block(line);
            let segment = source.segment;
            let first = self.visit_block(volume, at.column, segment, &line, |block| source.next_in_block(block))?;
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

    /// Run a function over a row block through the cache, and return nothing when its segment has left
    pub fn visit_block<Answer>(
        &self,
        volume: &Volume<Backend>,
        column: ColumnId,
        segment: SegmentNumber,
        line: &BlockEntry,
        mut visit: impl FnMut(&[u8]) -> Answer,
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
        self.footer_readers.with(|reader| match reader.load_block(&pin.file, key, line, &self.row_cache) {
            Ok(Some(block)) => Ok(Some(visit(block))),
            Ok(None) => Err(StoreError::Corruption { column }),
            Err(source) => Err(read_error(source)),
        })
    }
}

/// Report a failed read of a row block
pub fn read_error(source: Error) -> StoreError {
    StoreError::Io {
        action: "reading a row block",
        source,
    }
}
