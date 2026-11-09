//! Claiming room in an open segment, and opening, marking and sealing segments as they fill

use std::hint;
use std::io::{Error, ErrorKind};
use std::sync::atomic::Ordering;

use reel_api::column_id::ColumnId;
use reel_api::store_error::StoreError;
use reel_api::units::{ByteCount, DeathMark, FileOffset, Nanoseconds, RootId, RowCount, WritePlace};

use crate::engine::claim::Claim;
use crate::engine::ration::RATION_SPINS;
use crate::engine::engine_sizing::{OPEN_PER_ROOT, SEAL_ROWS_EIGHTHS};
use crate::engine::engine_state::{EngineState, IndexWait, SlotData};
use crate::engine::open_segment::{
    OpenSegment, RESERVE_STEP_BYTES, SegmentPhase, data_path,
    rows_path,
};
use crate::engine::volume::{ClaimState, Volume};
use crate::guard::hold;
use crate::index::open_slots::OpenSlot;
use crate::index::write_row::WriteRow;
use crate::lifetime::{MOST_LIFETIME_GROUPS, MarkRange, ceiling_of, reach_of};
use crate::io::{Io, IoFile, IoTask};
use crate::rows::row_group::{RowEntry, row_size};
use crate::segment::range_covers::MOST_RANGE_COVERS;

use crate::engine::write_path::{ClaimRequest, INDEX_WAIT, LandOutcome, RowRoom, SPARE_REQUEST, SegmentPick, WRITES_PER_OPEN, check_seal_room, check_writable, reserve_target, store_error};

impl<Backend: Io> EngineState<Backend> {
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
    #[cfg(any(test, feature = "test-hooks"))]
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
            is_room_holding: false,
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

