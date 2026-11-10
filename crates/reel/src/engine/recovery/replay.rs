//! Bringing open segments back from their rows files: the groups a sync covered, the records they list, and the seals that follow

use std::cmp::Reverse;
use std::io::{Error, ErrorKind, IoSlice, Result};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use reel_api::store_error::StoreError;
use reel_api::units::{ByteCount, FileOffset, Nanoseconds, RowCount, WritePlace};
use xxhash_rust::xxh3::Xxh3;

use crate::engine::engine_sizing::GROUP_BYTES;
use crate::engine::engine_state::{EngineState, SlotData};
use crate::engine::open_segment::{ROWS_SUFFIX, OpenSegment, SegmentPhase, rows_path};
#[cfg(any(test, feature = "test-hooks"))]
use crate::engine::open_segment::{DATA_SUFFIX, parse_segment};
use crate::engine::seal_job::SealJob;
use crate::engine::volume::Volume;
use crate::engine::write_path::{ClaimRequest, LandOutcome};
use crate::guard::hold;
use crate::index::open_slots::OpenSlot;
use crate::index::write_row::WriteRow;
use crate::lifetime::MarkRange;
use crate::io::job::{Job, JobStep};
use crate::io::device_error::{is_device_error, read_or_damaged};
use crate::io::{InPlace, Io, IoFile, in_place};
use crate::record::codec::Codec;
use crate::record::record_check::{Checksum, RecordIdentity, RecordKey, checksum};
use crate::record::record_head::SMALL_HEAD_BYTES;
use crate::record::record_reader::{RecordLocation, RecordRead, RecordReader};
use crate::row_kind::RowKind;
use crate::little_endian::read_half;
use crate::rows::row_group::{
    GROUP_HEADER_BYTES, GROUP_MAGIC, GROUP_SUM_BYTES, GroupHead, RowEntry, RowWalk, parse_group, parse_head,
};
use crate::segment::column_directory::Listing;
use crate::segment::damage_list::DamagedRange;
use crate::segment::range_covers::{Cover, RangeCovers, read_covers};
use crate::segment::segment_tail::{SegmentNumber, TailHead};

use crate::engine::recovery::{FoundSealed, GROUP_SEARCH_BYTES, MOST_INLINE_STEPS, OpenMode, RecoveredRows, RootScan, RowsScan, ScannedOpen, SegmentAt, damage_open, has_rows, holds_nothing, open_for, root_of_path, save_cleared_spans, whole_columns};

impl<Backend: Io> EngineState<Backend> {
    /// Put a sealed segment in the table and the directories
    pub fn install_sealed(&self, volume: &Volume<Backend>, found: &FoundSealed<Backend::File>) -> Result<()> {
        // a seal stopped after its tails reached the kernel leaves its rows, and a failed sync can have left
        // the tails nowhere else, so they go out again and sync before the rows go
        if found.has_rows && !self.is_read_only {
            let end = found.file.length()?;
            let tails = ByteCount(end.0 - found.head.second_index_at.0);
            rewrite_range(&found.file, found.head.second_index_at, tails, &mut vec![0; GROUP_BYTES.0 as usize])?;
            // the last sync covered the seal's cut to length too, so that goes out again as well
            found.file.truncate(end)?;
            found.file.sync()?;
            match self.io.remove_file(&rows_path(&found.path, found.head.segment)) {
                Ok(()) => {},
                Err(error) if error.kind() == ErrorKind::NotFound => {},
                Err(error) => return Err(error),
            }
        }

        volume.note_sealed(found.head.places.highest);
        for column in &found.columns {
            let entry = Listing::of_column(column, found.head.segment, found.head.places.highest);
            if let Some(directory) = &self.directories[column.column.0 as usize]
                && !directory.replace(Some(&entry), &[])
            {
                return Err(Error::other("column directory sized below what the roots hold"));
            }
        }
        // the table takes the file last, so a failure before it leaves one owner, the scan
        if !volume.table.install(found.file.clone(), root_of_path(volume, &found.path), &found.head, &found.columns, &found.entries) {
            return Err(Error::other("segment table sized below what the roots hold"));
        }
        Ok(())
    }

    /// Check a file is the one kind a full park may leave behind: with no record and no rows it can bring nothing back, and the
    /// next open removes it. Both have to hold, so nothing else passes
    pub fn is_empty_past_the_park(&self, path: &Path, is_park_full: bool) -> Result<bool> {
        if !is_park_full {
            return Ok(false);
        }
        let file = self.io.open_read_only(path)?;
        let is_empty = holds_nothing(&file);
        self.io.close_file(file);
        Ok(is_empty? && self.has_no_rows(path)?)
    }

    /// Check a data file's rows file is gone or holds nothing
    pub fn has_no_rows(&self, path: &Path) -> Result<bool> {
        let rows = path.with_extension(ROWS_SUFFIX.trim_start_matches('.'));
        let file = match self.io.open_read_only(&rows) {
            Ok(file) => file,
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(true),
            Err(error) => return Err(error),
        };
        let length = file.length();
        self.io.close_file(file);
        Ok(length?.0 == 0)
    }

    /// List the files with a segment's name that neither the lists nor a hold account for, which is always none at a quiet
    /// moment. A test asks this after every open and before every close
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn unlisted_segments(&self, volume: &Volume<Backend>) -> Result<Vec<SegmentNumber>> {
        let claims = hold(&volume.claims);
        let parked = hold(&volume.parked);
        let damaged = hold(&volume.damaged);
        let mut unlisted = Vec::new();
        for root in &volume.roots {
            for path in self.io.list_directory(root)? {
                let Some(number) = parse_segment(&path, DATA_SUFFIX) else {
                    continue;
                };
                let is_held = volume.table.holds(number)
                    || claims.open.iter().any(|segment| segment.number == number)
                    || claims.prepared.as_ref().is_some_and(|segment| segment.number == number)
                    || parked.iter().flatten().any(|parked| parked.segment == number)
                    || volume.lanes.iter().any(|lane| hold(&lane.partial_outputs).iter().flatten().any(|output| output.number == number))
                    || damaged.contains(&number);
                if !is_held && !self.is_empty_past_the_park(&path, parked.iter().all(Option::is_some))? {
                    unlisted.push(number);
                }
            }
        }
        Ok(unlisted)
    }

    /// Rebuild an open segment from its rows, ready to go on taking writes, or return false
    /// with nothing changed when its rows don't fit the write index
    pub fn recover_open(&self, volume: &Volume<Backend>, scanned: &ScannedOpen<Backend::File>) -> Result<bool> {
        let Some(slot) = self.take_slot(volume.id.0 as usize) else {
            return Err(Error::other("more open segments than slots"));
        };
        let found = &scanned.found;
        let mut recovered = RecoveredRows {
            write_end: FileOffset(0),
            row_count: RowCount(0),
            lowest_place: WritePlace(u64::MAX),
            highest_place: WritePlace(0),
            lost_region: None,
            cover_rows: 0,
            marks: MarkRange::NONE,
            has_deletes: false,
        };
        let mut records = RecordReader::new();
        let target = ReplayTarget {
            volume,
            data: &found.data,
            slot,
            segment: found.number,
        };
        let Some(kept_end) = self.replay_rows(&scanned.rows, &target, &scanned.scan, &mut records, &mut recovered)? else {
            self.index.clean_slot(slot, RowCount(u64::MAX));
            self.free_slot(slot);
            return Ok(false);
        };
        if let Some((start, end)) = recovered.lost_region {
            damage_open(self, found, rot_sum(scanned.scan.rot_sum, start, end))?;
        }
        scanned.rows.truncate(ByteCount(kept_end.0))?;

        let synced_through = FileOffset(scanned.scan.synced_through.0.min(kept_end.0));
        // what no mark covers may be a kernel's copy only, so it goes out again before the sync below
        let rewrite_from = synced_through;
        let unsynced = ByteCount(kept_end.0 - rewrite_from.0);
        rewrite_range(&scanned.rows, rewrite_from, unsynced, &mut vec![0; GROUP_BYTES.0 as usize])?;
        {
            let mut queue = hold(&self.queues[slot.0 as usize]);
            queue.reset(kept_end, synced_through);
            queue.marks = recovered.marks;
        }
        self.reset_windows(slot);
        // what open keeps, a read can see: this sync covers it on disk, so no crash or rot can take it back
        self.write_out_turn(slot, &scanned.rows, Some(&found.data))?;
        self.slot_files[slot.0 as usize].set(Some(SlotData {
            segment: found.number,
            file: found.data.clone(),
        }));
        volume.open_slot(slot);
        volume.rows_in_index.fetch_add(recovered.row_count.0, Ordering::Relaxed);
        hold(&volume.claims).open.push(OpenSegment {
            number: found.number,
            slot,
            root: found.root,
            data: found.data.clone(),
            rows: scanned.rows.clone(),
            phase: SegmentPhase::Taking,
            write_end: recovered.write_end,
            reserved_end: recovered.write_end,
            is_reserve_asked: false,
            is_room_holding: false,
            row_count: recovered.row_count,
            cover_rows: recovered.cover_rows,
            in_flight: 0,
            lowest_place: recovered.lowest_place,
            highest_place: recovered.highest_place,
            ceiling: recovered.marks.highest(),
            marks: recovered.marks,
            has_deletes: recovered.has_deletes,
            seen_rows: RowCount(0),
            quiet_since: Nanoseconds(0),
            first_row_at: None,
            delete_seen: None,
        });
        Ok(true)
    }

    /// Put each group's rows in the index, checking the records of groups no sync covered, and
    /// return where the kept rows end, or nothing when the index ran out of room
    pub fn replay_rows(
        &self,
        rows: &Backend::File,
        target: &ReplayTarget<Backend>,
        scan: &RowsScan,
        records: &mut RecordReader,
        recovered: &mut RecoveredRows,
    ) -> Result<Option<FileOffset>> {
        let mut bytes = vec![0; GROUP_BYTES.0 as usize];
        let mut copy = vec![0; GROUP_BYTES.0 as usize];
        let mut value = Vec::new();
        let mut at = FileOffset(0);
        let end = ByteCount(scan.valid_end.0);

        while at.0 < scan.valid_end.0 {
            let Some((start, head)) = next_group(rows, at, end, &mut bytes, self.record_key)? else {
                break;
            };
            at = start;

            // a group past the last sync may list a record a power cut lost, and it goes whole
            if at.0 >= scan.synced_through.0 {
                let walked = walk_rows(rows, at, &head, &mut bytes, |row| {
                    if !self.is_record_whole(target.data, row, records, &mut value)? {
                        return Ok(false);
                    }
                    rewrite_range(target.data, row.offset, row.length, &mut copy)?;
                    Ok(true)
                })?;
                match walked {
                    RowsWalked::Whole => {},
                    RowsWalked::Stopped => return Ok(Some(at)),
                    RowsWalked::Unreadable => {
                        recovered.lost_region = Some((at, FileOffset(at.0 + head.total_length.0)));
                        return Ok(Some(at));
                    },
                }
            }

            let walked = walk_rows(rows, at, &head, &mut bytes, |row| {
                Ok(self.replay_row(row, target, recovered, records, &mut value))
            })?;
            match walked {
                RowsWalked::Whole => {},
                RowsWalked::Stopped => return Ok(None),
                // rows of this group are in the index, and damage over the columns keeps them from a read
                RowsWalked::Unreadable => recovered.lost_region = Some((at, FileOffset(at.0 + head.total_length.0))),
            }
            recovered.marks = recovered.marks.join(head.marks);
            at = FileOffset(at.0 + head.total_length.0);
        }
        Ok(Some(at))
    }

    /// Check a row's record reads back whole, and take a row with no bytes as it stands
    pub fn is_record_whole(
        &self,
        data: &Backend::File,
        row: &RowEntry,
        records: &mut RecordReader,
        value: &mut Vec<u8>,
    ) -> Result<bool> {
        if row.kind == RowKind::Grave {
            return Ok(true);
        }

        let identity = RecordIdentity {
            record_key: self.record_key,
            column: row.column,
            key: row.key,
            kind: row.kind,
            place: row.place,
        };
        let location = RecordLocation {
            offset: row.offset,
            length: row.length,
        };
        let Some(info) = self.columns[row.column.0 as usize] else {
            return Ok(false);
        };
        // the record of a range delete holds its end, stored as written whatever the column does
        let codec = if row.kind == RowKind::RangeDelete { Codec::None } else { info.codec };
        // a row whose record the device can't give back has no whole write behind it
        let read = in_place(records.read_value(&InPlace, data, location, &identity, codec, value));
        match read {
            Ok(RecordRead::Valid) => Ok(true),
            Ok(RecordRead::Failed) => Ok(false),
            Err(error) if is_device_error(&error) => Ok(false),
            Err(error) => Err(error),
        }
    }

    /// Bring a recovered range delete back into the pool, its end read from its record
    pub fn replay_cover(
        &self,
        row: &RowEntry,
        target: &ReplayTarget<Backend>,
        recovered: &mut RecoveredRows,
        records: &mut RecordReader,
        value: &mut Vec<u8>,
    ) {
        // a cover no read can give back could have hidden anything, so the columns are damaged whole
        let lost = Some((row.offset, FileOffset(row.offset.0 + row.length.0)));
        if !matches!(self.is_record_whole(target.data, row, records, value), Ok(true)) {
            recovered.lost_region = lost;
            return;
        }

        let cover = Cover {
            column: row.column,
            start: row.key,
            end: value,
            place: row.place,
            segment: target.segment,
        };
        let has_room = target.volume.covers.add(&cover, false);
        if !has_room {
            recovered.lost_region = lost;
        }
    }

    /// Put one recovered row in the write index, and return false when it has no room
    pub fn replay_row(
        &self,
        row: &RowEntry,
        target: &ReplayTarget<Backend>,
        recovered: &mut RecoveredRows,
        records: &mut RecordReader,
        value: &mut Vec<u8>,
    ) -> bool {
        let write_row = WriteRow {
            place: row.place,
            offset: row.offset,
            length: row.length,
            kind: row.kind,
            slot: target.slot,
            segment: target.segment,
            has_newer_sealed: target.volume.may_hide_under_sealed(row.place),
        };
        if !self.index.insert(row.column, row.key, &write_row) {
            return false;
        }
        if row.kind == RowKind::RangeDelete {
            recovered.cover_rows += 1;
            self.replay_cover(row, target, recovered, records, value);
        }

        recovered.row_count = RowCount(recovered.row_count.0 + 1);
        recovered.has_deletes |= row.kind != RowKind::Value;
        recovered.lowest_place = recovered.lowest_place.min(row.place);
        recovered.highest_place = recovered.highest_place.max(row.place);
        recovered.write_end = recovered.write_end.max(FileOffset(row.offset.0 + row.length.0));
        true
    }

    /// Carry on the segment numbers from what open found
    pub fn resume_numbers(&self, highest_segment: SegmentNumber) {
        self.next_segment.fetch_max(highest_segment.0 + 1, Ordering::AcqRel);
    }
}

/// Write a range back over itself: a kernel forgets the writes a failed sync covered and keeps showing
/// them, so bytes no sync covered must be dirty again before a later sync can say they're on disk
pub fn rewrite_range<File: IoFile>(file: &File, start: FileOffset, length: ByteCount, copy: &mut [u8]) -> Result<()> {
    let mut at = start.0;
    let end = start.0 + length.0;
    while at < end {
        let step = copy.len().min((end - at) as usize);
        // a bad sector has nothing to give back, and what's under it stays as it is
        if read_or_damaged(file, FileOffset(at), &mut copy[..step])? {
            file.write_at(FileOffset(at), &[IoSlice::new(&copy[..step])])?;
        }
        at += step as u64;
    }
    Ok(())
}

/// Add the covers of a footer to the pool, or return false when it fails or the pool has no slot left
pub fn read_block(bytes: &[u8], segment: SegmentNumber, covers: &RangeCovers, is_applied: bool) -> bool {
    let mut has_room = true;
    let is_whole = read_covers(bytes, segment, |cover| has_room &= covers.add(cover, is_applied));
    is_whole && has_room
}

/// Bring back the covers sealed footers keep. A block that fails its check can't say what it hid, so its segment's
/// columns are damaged whole
pub fn load_covers<Backend: Io>(state: &EngineState<Backend>, scan: &RootScan<Backend::File>) -> Result<()> {
    // newest first, so a cover inside a newer one never takes a slot
    let mut sealed: Vec<&FoundSealed<Backend::File>> = scan.sealed.iter().collect();
    sealed.sort_by_key(|found| Reverse(found.head.places.highest));
    let mut bytes = Vec::new();
    for found in sealed {
        let volume = &state.volumes[found.volume];
        // a row with the flag gone says its covers settled, so they stay out of the pool. A row an earlier seal of this number
        // left says nothing about these covers, so it has to be this tail's
        let head = &found.head;
        let is_settled = hold(&volume.summary).get(head.segment).is_some_and(|row| {
            !row.facts.has_covers && row.facts.seal_mark == head.index_sum && row.facts.places == head.places
        });
        if found.head.ranges_length.0 == 0 || is_settled {
            continue;
        }
        bytes.resize(found.head.ranges_length.0 as usize, 0);
        let is_read = read_or_damaged(&found.file, found.head.ranges_at, &mut bytes)?;
        // a saved walk has counted what these covers hide, so an older one stays beside a newer one that hasn't
        let is_applied = hold(&volume.summary).get(found.head.segment).is_some_and(|row| row.facts.is_walked());
        let is_whole = is_read
            && checksum(&bytes) == found.head.ranges_sum
            && read_block(&bytes, found.head.segment, &volume.covers, is_applied);
        if !is_whole {
            hold(&volume.lost_rows).unread_covers.push(found.head.segment);
        }
    }
    Ok(())
}

/// Move the order past what the open segments hold, before anything ranks damage. The limit should already stand above these,
/// and open never hands out a place it found
pub fn pass_open_rows<Backend: Io>(state: &EngineState<Backend>, scan: &RootScan<Backend::File>) {
    for segment in &scan.scanned {
        state.resume_past(segment.scan.highest_place);
    }
}

/// Bring every open segment back, sealing as a running store at this budget would have, taking each out of
/// the scan once its files have a new owner
pub fn recover_segments<Backend: Io>(
    state: &Arc<EngineState<Backend>>,
    scan: &mut RootScan<Backend::File>,
) -> Result<()> {
    state.resume_numbers(scan.highest_segment);
    for segment in &scan.scanned {
        if segment.scan.has_rot {
            damage_open(state, &segment.found, segment.scan.rot_sum)?;
        }
    }

    scan.scanned.reverse();
    while let Some(segment) = scan.scanned.last() {
        let is_kept = recover_one(state, segment)?;
        if let Some(segment) = scan.scanned.pop()
            && !is_kept
        {
            drop_copied(state, segment)?;
        }
    }
    Ok(())
}

/// Recover one open segment, and say whether it went on as an open segment or was copied out
pub fn recover_one<Backend: Io>(state: &Arc<EngineState<Backend>>, segment: &ScannedOpen<Backend::File>) -> Result<bool> {
    let index = segment.found.volume;
    let volume = &state.volumes[index];
    // a segment made ahead that no claim took has an empty rows file. Bytes that fail their check are something else
    if segment.rows.length()?.0 == 0 && hold(&volume.summary).get(segment.found.number).is_none() {
        return Ok(false);
    }
    if segment.scan.row_count > volume.segment_rows {
        copy_segment(state, index, segment)?;
        return Ok(false);
    }

    make_room(state, index, segment.scan.row_count)?;
    // failed removes can leave more open segments than a store has slots, and a seal gives one back
    if !state.has_free_slot() {
        for other in 0..state.volumes.len() {
            seal_every_open(state, other)?;
        }
    }
    if state.recover_open(volume, segment)? {
        return Ok(true);
    }

    // long keys can fill the index below the row limit
    seal_every_open(state, index)?;
    if state.recover_open(volume, segment)? {
        return Ok(true);
    }
    copy_segment(state, index, segment)?;
    Ok(false)
}

/// Close and remove a segment whose rows now live in copies
pub fn drop_copied<Backend: Io>(state: &Arc<EngineState<Backend>>, segment: ScannedOpen<Backend::File>) -> Result<()> {
    let target = &state.volumes[segment.found.volume];
    let path = &target.roots[segment.found.root.0 as usize];
    let number = segment.found.number;
    state.io.close_file(segment.found.data);
    state.io.close_file(segment.rows);
    // a data file with no rows and no tail reads as damage, so the rows go only once it has
    let has_room = target.can_park(1);
    if !state.remove_or_park(target, path, number) {
        // only an empty file may stay in no list, so a copied one that won't go ends the open
        if !has_room {
            return Err(Error::from(ErrorKind::StorageFull));
        }
        return Ok(());
    }
    state.io.remove_file(&rows_path(path, number))?;
    state.io.sync_directory(path)
}

/// Seal the fullest segments until some rows fit under the volume's limit
pub fn make_room<Backend: Io>(state: &Arc<EngineState<Backend>>, volume: usize, rows: RowCount) -> Result<()> {
    let target = &state.volumes[volume];
    loop {
        let mut open_rows = rows.0;
        let mut has_open = false;
        for segment in &hold(&target.claims).open {
            open_rows += segment.row_count.0;
            has_open = true;
        }
        if open_rows <= target.segment_rows.0 || !has_open {
            return Ok(());
        }
        state.seal_fullest(target);
        seal_inline(state, volume)?;
    }
}

/// Seal every open segment of a volume
pub fn seal_every_open<Backend: Io>(state: &Arc<EngineState<Backend>>, volume: usize) -> Result<()> {
    let target = &state.volumes[volume];
    while !hold(&target.claims).open.is_empty() {
        state.seal_fullest(target);
        // the fullest pick skips a segment with no rows, and those leave by the same seal
        state.seal_empty(target);
        seal_inline(state, volume)?;
    }
    Ok(())
}

/// Run the seals marked on a volume to their end, on the opening thread
pub fn seal_inline<Backend: Io>(state: &Arc<EngineState<Backend>>, volume: usize) -> Result<()> {
    let mut job = SealJob::new(Arc::clone(state), volume);
    for _step in 0..MOST_INLINE_STEPS {
        match job.step() {
            JobStep::Worked => {},
            JobStep::IdleUntil { .. } => state.io.pause(),
            JobStep::Idle | JobStep::Finished => {
                if state.volumes[volume].has_failed() {
                    return Err(Error::other("a seal failed during open"));
                }
                return Ok(());
            },
        }
    }
    Err(Error::other("a seal never finished during open"))
}

/// Copy a segment too large for this budget's index into new segments, keeping every place, so
/// the caller can drop it: a crash part way leaves it whole, and the copies only repeat rows it still holds
pub fn copy_segment<Backend: Io>(
    state: &Arc<EngineState<Backend>>,
    volume: usize,
    segment: &ScannedOpen<Backend::File>,
) -> Result<()> {
    let mut bytes = vec![0; GROUP_BYTES.0 as usize];
    let mut value = Vec::new();
    let mut record = Vec::new();
    let mut records = RecordReader::new();
    let mut at = FileOffset(0);
    let end = ByteCount(segment.scan.valid_end.0);

    while at.0 < segment.scan.valid_end.0 {
        let Some((start, head)) = next_group(&segment.rows, at, end, &mut bytes, state.record_key)? else {
            break;
        };
        at = start;

        if at.0 >= segment.scan.synced_through.0 {
            let walked = walk_rows(&segment.rows, at, &head, &mut bytes, |row| {
                state.is_record_whole(&segment.found.data, row, &mut records, &mut value)
            })?;
            if walked == RowsWalked::Unreadable {
                damage_open(state, &segment.found, rot_sum(segment.scan.rot_sum, at, FileOffset(at.0 + head.total_length.0)))?;
            }
            if walked != RowsWalked::Whole {
                break;
            }
        }

        let walked = walk_rows(&segment.rows, at, &head, &mut bytes, |row| {
            // a cover that can't be read could have hidden anything, so the columns go whole and it isn't copied
            if row.kind == RowKind::RangeDelete
                && !state.is_record_whole(&segment.found.data, row, &mut records, &mut value)?
            {
                let end = FileOffset(row.offset.0 + row.length.0);
                damage_open(state, &segment.found, rot_sum(segment.scan.rot_sum, row.offset, end))?;
                return Ok(true);
            }
            copy_row(state, volume, segment.found.number, &segment.found.data, row, &mut record)?;
            Ok(true)
        })?;
        if walked == RowsWalked::Unreadable {
            damage_open(state, &segment.found, rot_sum(segment.scan.rot_sum, at, FileOffset(at.0 + head.total_length.0)))?;
        }
        at = FileOffset(at.0 + head.total_length.0);
    }

    let target = &state.volumes[volume];
    state.sync_volume(target).map_err(open_failure)
}

/// Keep a row whose record the device lost out of the copy, and damage its key from here on, ranked at the
/// row, so nothing older answers for it
pub fn damage_key<Backend: Io>(
    state: &EngineState<Backend>,
    volume: &Volume<Backend>,
    segment: SegmentNumber,
    row: &RowEntry,
) -> Result<()> {
    let mut end = row.key.to_vec();
    end.push(0);
    volume.damage.add(&DamagedRange {
        column: row.column,
        segment,
        highest_place: row.place,
        start: row.key.to_vec(),
        end: Some(end),
    });
    let mut damaged = hold(&volume.damaged);
    if !damaged.contains(&segment) {
        damaged.push(segment);
    }
    drop(damaged);
    save_cleared_spans(state, volume);
    state.save_summary(volume)
}

/// Copy one row and its record as stored, sealing when the limits say a running store would
pub fn copy_row<Backend: Io>(
    state: &Arc<EngineState<Backend>>,
    volume: usize,
    segment: SegmentNumber,
    data: &Backend::File,
    row: &RowEntry,
    record: &mut Vec<u8>,
) -> Result<()> {
    let target = &state.volumes[volume];
    record.resize(row.length.0 as usize, 0);
    if !record.is_empty() && !read_or_damaged(data, row.offset, record)? {
        return damage_key(state, target, segment, row);
    }

    let request = ClaimRequest {
        bytes: row.length,
        rows: RowCount(1),
        is_alone: false,
        has_records: row.length.0 > 0,
        covers: usize::from(row.kind == RowKind::RangeDelete),
        mark: None,
        has_deletes: row.kind != RowKind::Value,
    };
    loop {
        let claim = state.claim(target, &request).map_err(open_failure)?;
        if !record.is_empty() {
            claim.data.write_at(claim.offset, &[IoSlice::new(record)])?;
        }

        let copied = WriteRow {
            place: row.place,
            offset: claim.offset,
            length: row.length,
            kind: row.kind,
            slot: claim.slot,
            segment: claim.segment,
            has_newer_sealed: target.may_hide_under_sealed(row.place),
        };
        match state.land_row(row.column, row.key, &copied).map_err(open_failure)? {
            LandOutcome::Landed => {
                note_copied_place(target, copied.slot, row.place);
                if row.kind == RowKind::RangeDelete {
                    let cover = Cover {
                        column: row.column,
                        start: row.key,
                        end: record.get(SMALL_HEAD_BYTES..).unwrap_or_default(),
                        place: row.place,
                        segment: copied.segment,
                    };
                    let has_room = target.covers.add(&cover, false);
                    if !has_room {
                        // the cover's row stays in the copy, so this holds for this open and the next open decides again
                        for range in whole_columns(state, volume, copied.segment, state.places.last_given()) {
                            target.damage.add(&range);
                        }
                    }
                }
                claim.finish();
                break;
            },
            LandOutcome::IndexFull => {
                drop(claim);
                seal_every_open(state, volume)?;
            },
        }
    }
    let is_sealing = hold(&target.claims).open.iter().any(|segment| segment.phase == SegmentPhase::Sealing);
    if is_sealing {
        seal_inline(state, volume)?;
    }
    Ok(())
}

/// Lower a segment's first place to a copied row's, which predates the segment
pub fn note_copied_place<Backend: Io>(volume: &Volume<Backend>, slot: OpenSlot, place: WritePlace) {
    for segment in &mut hold(&volume.claims).open {
        if segment.slot == slot {
            segment.lowest_place = segment.lowest_place.min(place);
        }
    }
}

/// Carry a store error out through open's own errors
pub fn open_failure(error: StoreError) -> Error {
    Error::other(error.to_string())
}

/// Where recovered rows go
pub struct ReplayTarget<'file, Backend: Io> {
    pub volume: &'file Volume<Backend>,
    pub data: &'file Backend::File,
    pub slot: OpenSlot,
    pub segment: SegmentNumber,
}


/// Read the group at an offset into a buffer, and return its length when it's all there
pub fn read_group<File: IoFile>(
    rows: &File,
    at: FileOffset,
    end: ByteCount,
    bytes: &mut [u8],
    record_key: RecordKey,
) -> Result<Option<GroupHead>> {
    let left = end.0.saturating_sub(at.0);
    if left < GROUP_HEADER_BYTES as u64 || !read_or_damaged(rows, at, &mut bytes[..GROUP_HEADER_BYTES])? {
        return Ok(None);
    }
    let Some(head) = parse_head(bytes) else {
        return Ok(None);
    };
    if head.total_length.0 > left {
        return Ok(None);
    }

    let length = head.total_length.0 as usize;
    if length > bytes.len() {
        let is_whole = is_long_group_whole(rows, at, length, bytes, record_key)?;
        return Ok(is_whole.then_some(head));
    }
    if !read_or_damaged(rows, at, &mut bytes[..length])? {
        return Ok(None);
    }
    Ok(parse_group(&bytes[..length], record_key).map(|_group| head))
}

/// Check a group longer than the buffer against its checksum, a piece at a time
pub fn is_long_group_whole<File: IoFile>(
    rows: &File,
    at: FileOffset,
    length: usize,
    bytes: &mut [u8],
    record_key: RecordKey,
) -> Result<bool> {
    let mut sum = Xxh3::with_seed(record_key.0);
    let body = length - GROUP_SUM_BYTES;
    let mut done = 0;
    while done < body {
        let step = bytes.len().min(body - done);
        if !read_or_damaged(rows, FileOffset(at.0 + done as u64), &mut bytes[..step])? {
            return Ok(false);
        }
        sum.update(&bytes[..step]);
        done += step;
    }

    let mut stored = [0; GROUP_SUM_BYTES];
    let is_read = read_or_damaged(rows, FileOffset(at.0 + body as u64), &mut stored)?;
    Ok(is_read && u64::from_le_bytes(stored) == sum.digest())
}

/// How a walk over a group's rows ended
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RowsWalked {
    /// Every row went through the work
    Whole,
    /// The work said stop
    Stopped,
    /// The device lost a piece the group's check had just read
    Unreadable,
}

/// Walk the rows of a group `read_group` passed, until the work says stop. A short group is still in the
/// buffer, and a longer one comes in pieces, with the row a piece cut in two carried to the next
pub fn walk_rows<File: IoFile>(
    rows: &File,
    at: FileOffset,
    head: &GroupHead,
    bytes: &mut [u8],
    mut work: impl FnMut(&RowEntry) -> Result<bool>,
) -> Result<RowsWalked> {
    let length = head.total_length.0 as usize;
    let rows_end = length - GROUP_SUM_BYTES;
    if length <= bytes.len() {
        for row in RowWalk::over(&bytes[GROUP_HEADER_BYTES..rows_end]) {
            if !work(&row)? {
                return Ok(RowsWalked::Stopped);
            }
        }
        return Ok(RowsWalked::Whole);
    }

    let mut done = GROUP_HEADER_BYTES;
    let mut carried = 0;
    while done < rows_end {
        let step = (bytes.len() - carried).min(rows_end - done);
        // a row no walk can read fills the buffer, and a group that passed its check can't hold one
        if step == 0 {
            return Err(Error::other("a group's check passed over a row no walk can read"));
        }
        if !read_or_damaged(rows, FileOffset(at.0 + done as u64), &mut bytes[carried..carried + step])? {
            return Ok(RowsWalked::Unreadable);
        }
        done += step;
        let filled = carried + step;
        let mut walk = RowWalk::over(&bytes[..filled]);
        for row in walk.by_ref() {
            if !work(&row)? {
                return Ok(RowsWalked::Stopped);
            }
        }
        carried = walk.left();
        bytes.copy_within(filled - carried..filled, 0);
    }
    Ok(RowsWalked::Whole)
}

/// Fold a bad region into a sum of where a rows file's bad regions lie, through a real hash, so a region that
/// grows or a second region changes it, and never to the zero a row with no rot carries
pub fn rot_sum(sum: Checksum, start: FileOffset, end: FileOffset) -> Checksum {
    let mut region = [0; 3 * size_of::<u64>()];
    region[..8].copy_from_slice(&sum.0.to_le_bytes());
    region[8..16].copy_from_slice(&start.0.to_le_bytes());
    region[16..].copy_from_slice(&end.0.to_le_bytes());
    Checksum(checksum(&region).0 | 1)
}

/// Check whether the device can't give back some sector of a range
pub fn has_bad_sector<File: IoFile>(rows: &File, start: FileOffset, end: ByteCount) -> Result<bool> {
    let mut sector = [0; GROUP_SEARCH_BYTES];
    let mut at = start.0;
    while at < end.0 {
        let sector_end = (at / GROUP_SEARCH_BYTES as u64 + 1) * GROUP_SEARCH_BYTES as u64;
        let length = (sector_end.min(end.0) - at) as usize;
        if !read_or_damaged(rows, FileOffset(at), &mut sector[..length])? {
            return Ok(true);
        }
        at = sector_end;
    }
    Ok(false)
}

/// Check the header at an offset could open a group that covers a bad offset
pub fn is_covering_head<File: IoFile>(
    rows: &File,
    candidate: FileOffset,
    bad_at: FileOffset,
    bytes: &mut [u8],
) -> Result<bool> {
    if !read_or_damaged(rows, candidate, &mut bytes[..GROUP_HEADER_BYTES])? {
        return Ok(false);
    }
    Ok(parse_head(bytes).is_some_and(|head| head.synced_through > bad_at && head.synced_through <= candidate))
}

/// Search past a bad group, a sector at a time, for a whole group whose sync mark reaches over it
pub fn covering_group<File: IoFile>(
    rows: &File,
    bad_at: FileOffset,
    end: ByteCount,
    bytes: &mut [u8],
    record_key: RecordKey,
) -> Result<Option<(FileOffset, GroupHead)>> {
    let mut chunk = [0; GROUP_SEARCH_BYTES + size_of::<u32>()];
    let mut at = bad_at.0 + 1;
    while at + size_of::<u32>() as u64 <= end.0 {
        let sector_end = (at / GROUP_SEARCH_BYTES as u64 + 1) * GROUP_SEARCH_BYTES as u64;
        let length = (sector_end.min(end.0) - at) as usize;
        // a bad sector hides what it holds, and the search goes on at the next one
        if read_or_damaged(rows, FileOffset(at), &mut chunk[..length])? {
            // a marker can start in this sector and run into the next, which may not read
            let reach = (length + size_of::<u32>() - 1).min((end.0 - at) as usize);
            let is_whole = read_or_damaged(rows, FileOffset(at), &mut chunk[..reach])?;
            let readable = if is_whole { reach } else { length };
            for position in 0..=readable.saturating_sub(size_of::<u32>()) {
                if read_half(&chunk, position) != Some(GROUP_MAGIC) {
                    continue;
                }
                let candidate = FileOffset(at + position as u64);
                // a real group's mark reaches past the bad bytes and no further than its own start, so a stray
                // marker in rotted bytes costs a header read and no hash
                if !is_covering_head(rows, candidate, bad_at, bytes)? {
                    continue;
                }
                if let Some(head) = read_group(rows, candidate, end, bytes, record_key)? {
                    return Ok(Some((candidate, head)));
                }
            }
        }
        at = sector_end;
    }
    Ok(None)
}

/// Find the next whole group: the one at this offset, or past a bad one the first that shows a sync covered it, which makes
/// the bad one rot and never a torn end
pub fn next_group<File: IoFile>(
    rows: &File,
    at: FileOffset,
    end: ByteCount,
    bytes: &mut [u8],
    record_key: RecordKey,
) -> Result<Option<(FileOffset, GroupHead)>> {
    if let Some(head) = read_group(rows, at, end, bytes, record_key)? {
        return Ok(Some((at, head)));
    }
    covering_group(rows, at, end, bytes, record_key)
}

/// Walk a rows file for what recovery needs to know before it reads any record
pub fn scan_rows_of<File: IoFile>(rows: &File, record_key: RecordKey) -> Result<RowsScan> {
    let mut scan = RowsScan::default();
    let mut bytes = vec![0; GROUP_BYTES.0 as usize];
    let length = rows.length()?;

    while scan.valid_end.0 < length.0 {
        let Some((at, head)) = next_group(rows, scan.valid_end, length, &mut bytes, record_key)? else {
            break;
        };
        if at != scan.valid_end {
            scan.has_rot = true;
            scan.rot_sum = rot_sum(scan.rot_sum, scan.valid_end, at);
        }
        scan.synced_through = scan.synced_through.max(head.synced_through);
        scan.row_count = RowCount(scan.row_count.0 + head.row_count.0);
        let mut highest = scan.highest_place;
        let walked = walk_rows(rows, at, &head, &mut bytes, |row| {
            highest = highest.max(row.place);
            Ok(true)
        })?;
        if walked == RowsWalked::Unreadable {
            scan.has_rot = true;
            scan.rot_sum = rot_sum(scan.rot_sum, at, FileOffset(at.0 + head.total_length.0));
        }
        scan.highest_place = highest;
        scan.valid_end = FileOffset(at.0 + head.total_length.0);
    }
    // a torn end is bytes that read and fail their check, so a device error past the last good group is rot
    if scan.valid_end.0 < length.0 && has_bad_sector(rows, scan.valid_end, length)? {
        scan.has_rot = true;
        scan.rot_sum = rot_sum(scan.rot_sum, scan.valid_end, FileOffset(length.0));
    }
    Ok(scan)
}

/// Check a segment's rows file goes past what its tail lists. A seal that stopped after its tail leaves those bytes in the file
/// when the segment goes on taking writes, and a power cut that loses what came after can bring them back to the file's end.
/// The tails sync before the cut for the same reason, and this covers every other way a tail that never finished reads as final
pub fn is_tail_stale(head: &TailHead, walked: &RowsScan) -> bool {
    walked.highest_place > head.places.highest
}

/// Open and walk the rows file a listing shows. A view finds it gone once a seal finishes
pub fn walk_listed_rows<Backend: Io>(io: &Backend, at: &SegmentAt) -> Result<Option<(Backend::File, RowsScan)>> {
    if !has_rows(at.listing, at.number) {
        return Ok(None);
    }
    let rows = match open_for(io, &rows_path(&at.listing.path, at.number), at.mode) {
        Ok(rows) => rows,
        Err(error) if at.mode == OpenMode::ReadOnly && error.kind() == ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    match scan_rows_of(&rows, at.record_key) {
        Ok(walked) => Ok(Some((rows, walked))),
        Err(error) => {
            io.close_file(rows);
            Err(error)
        },
    }
}
