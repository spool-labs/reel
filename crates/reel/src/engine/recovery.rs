//! Open's reading of the roots: sealed segments from their tails, open ones from their rows

use std::collections::BTreeSet;
use std::io::{Error, ErrorKind, IoSlice, Result};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use reel_api::column_id::ColumnId;
use reel_api::store_error::StoreError;
use reel_api::units::{ByteCount, FileOffset, RootId, RowCount, WritePlace};

use crate::config::MAX_KEY_BYTES;
use crate::engine::engine_sizing::GROUP_BYTES;
use crate::engine::engine_state::{EngineState, SlotData};
use crate::engine::open_segment::{DATA_SUFFIX, OpenSegment, SegmentPhase, data_path, parse_segment, rows_path};
use crate::engine::seal_job::SealJob;
use crate::engine::volume::Volume;
use crate::engine::write_path::{ClaimRequest, LandOutcome};
use crate::guard::hold;
use crate::index::open_slots::OpenSlot;
use crate::index::write_row::WriteRow;
use crate::io::job::{Job, JobStep};
use crate::io::{Io, IoFile};
use crate::record::record_check::{RecordIdentity, RecordKey};
use crate::record::record_head::SMALL_HEAD_BYTES;
use crate::record::record_reader::{RecordLocation, RecordRead, RecordReader};
use crate::row_kind::RowKind;
use crate::little_endian::read_half;
use crate::rows::row_group::{GROUP_HEADER_BYTES, RowEntry, TOTAL_LENGTH_AT, parse_group};
use crate::segment::block_entry::BlockEntry;
use crate::segment::column_directory::DirectoryEntry;
use crate::segment::damage_list::DamagedRange;
use crate::segment::footer_reader::FooterReader;
use crate::segment::segment_table::{TABLE_SHARDS, TableSizing, shard_of};
use crate::segment::segment_tail::{ColumnTail, SegmentNumber, TailHead};
use crate::segment::summary::{
    ColumnFacts, SegmentFacts, SpanFacts, SummaryPaths, SummaryRow, SummaryRows, read_summary,
};

/// Steps a seal on the opening thread can take before open reports it stuck
pub const MOST_INLINE_STEPS: u64 = 100_000_000;

/// A root's files, listed before any of them is read
#[derive(Clone, Debug)]
pub struct RootListing {
    pub volume: usize,
    pub root: RootId,
    pub path: PathBuf,
    pub files: Vec<PathBuf>,
}

/// A segment whose tail is valid
#[derive(Debug)]
pub struct FoundSealed<File> {
    pub volume: usize,
    pub path: PathBuf,
    pub file: File,
    pub head: TailHead,
    pub columns: Vec<ColumnTail>,
    pub entries: Vec<BlockEntry>,
}

/// A sealed segment whose copies of the tail or the index all fail
#[derive(Clone, Debug)]
pub struct FoundDamaged {
    pub volume: usize,
    pub number: SegmentNumber,
    pub file_length: ByteCount,
    pub head: Option<TailHead>,
    pub columns: Vec<ColumnTail>,
}

/// A column's facts with its keys copied out of the summary
#[derive(Clone, Debug)]
pub struct OwnedColumn {
    pub column: ColumnId,
    pub lowest: Vec<u8>,
    pub highest: Vec<u8>,
    pub row_count: RowCount,
    pub grave_count: RowCount,
}

/// A volume's summary as open found it
#[derive(Debug)]
pub struct SummaryFound {
    pub rows: SummaryRows,
    pub paths: SummaryPaths,
}

/// A segment with no valid tail and a rows file
#[derive(Debug)]
pub struct FoundOpen<File> {
    pub volume: usize,
    pub root: RootId,
    pub number: SegmentNumber,
    pub data: File,
}

/// What a read of every root found
#[derive(Debug)]
pub struct RootScan<File> {
    pub sealed: Vec<FoundSealed<File>>,
    pub open: Vec<FoundOpen<File>>,
    pub damaged: Vec<FoundDamaged>,
    pub highest_segment: SegmentNumber,
}

/// An open segment with its rows file walked, waiting for its turn
#[derive(Debug)]
pub struct ScannedOpen<File> {
    pub found: FoundOpen<File>,
    pub rows: File,
    pub scan: RowsScan,
}

/// What a walk of one rows file found
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RowsScan {
    pub valid_end: FileOffset,
    pub synced_through: FileOffset,
    pub row_count: RowCount,
}

/// The rows of a recovered segment so far
#[derive(Clone, Copy, Debug)]
pub struct RecoveredRows {
    pub write_end: FileOffset,
    pub row_count: RowCount,
    pub lowest_place: WritePlace,
    pub highest_place: WritePlace,
}

impl<File> RootScan<File> {
    /// Work out the table room each volume needs for what was found
    pub fn least_sizing(&self, volume: usize) -> TableSizing {
        let mut segments = [0; TABLE_SHARDS];
        let mut columns = [0; TABLE_SHARDS];
        let mut blocks = [0; TABLE_SHARDS];
        for found in &self.sealed {
            if found.volume == volume {
                let shard = shard_of(found.head.segment);
                segments[shard] += 1;
                columns[shard] += found.columns.len();
                blocks[shard] += found.entries.len();
            }
        }

        let mut sizing = TableSizing {
            segments_per_shard: 0,
            columns_per_shard: 0,
            blocks_per_shard: 0,
        };
        for shard in 0..TABLE_SHARDS {
            sizing.segments_per_shard = sizing.segments_per_shard.max(segments[shard]);
            sizing.columns_per_shard = sizing.columns_per_shard.max(columns[shard]);
            sizing.blocks_per_shard = sizing.blocks_per_shard.max(blocks[shard]);
        }
        sizing
    }
}

/// List every root of every volume, before any tail is read
pub fn list_roots<Backend: Io>(io: &Backend, roots: &[(usize, Vec<PathBuf>)]) -> Result<Vec<RootListing>> {
    let mut listings = Vec::new();
    for (volume, paths) in roots {
        for (root, path) in paths.iter().enumerate() {
            listings.push(RootListing {
                volume: *volume,
                root: RootId(root as u8),
                path: path.clone(),
                files: io.list_directory(path)?,
            });
        }
    }
    Ok(listings)
}

/// Read every listed segment's tail and sort them into sealed, open and damaged
pub fn scan_roots<Backend: Io>(
    io: &Backend,
    record_key: RecordKey,
    listings: &[RootListing],
) -> Result<RootScan<Backend::File>> {
    let mut scan = RootScan {
        sealed: Vec::new(),
        open: Vec::new(),
        damaged: Vec::new(),
        highest_segment: SegmentNumber(0),
    };
    let mut reader = FooterReader::new();

    for listing in listings {
        for path in &listing.files {
            let Some(number) = parse_segment(path, DATA_SUFFIX) else {
                continue;
            };
            scan.highest_segment = scan.highest_segment.max(number);
            let file = io.open_file(path)?;
            sort_segment(io, record_key, listing, number, file, &mut reader, &mut scan)?;
        }
    }
    Ok(scan)
}

/// Sort one segment by what its tail and its rows file say
pub fn sort_segment<Backend: Io>(
    io: &Backend,
    record_key: RecordKey,
    listing: &RootListing,
    number: SegmentNumber,
    file: Backend::File,
    reader: &mut FooterReader,
    scan: &mut RootScan<Backend::File>,
) -> Result<()> {
    let mut columns = Vec::new();
    if let Some(head) = reader.read_tail(&file, record_key, &mut columns)? {
        let mut entries = Vec::new();
        if !reader.read_index(&file, &head, &mut entries)? {
            // the file stays, and open decides what its ranges are once it has read the summary
            scan.damaged.push(FoundDamaged {
                volume: listing.volume,
                number,
                file_length: file.length()?,
                head: Some(head),
                columns,
            });
            return Ok(());
        }
        scan.sealed.push(FoundSealed {
            volume: listing.volume,
            path: listing.path.clone(),
            file,
            head,
            columns,
            entries,
        });
        return Ok(());
    }

    if listing.files.contains(&rows_path(&listing.path, number)) {
        scan.open.push(FoundOpen {
            volume: listing.volume,
            root: listing.root,
            number,
            data: file,
        });
        return Ok(());
    }

    let is_empty = holds_nothing(&file)?;
    let file_length = file.length()?;
    io.close_file(file);
    if is_empty {
        // a crash between creating the two files of a segment leaves this
        io.remove_file(&data_path(&listing.path, number))?;
        return Ok(());
    }

    scan.damaged.push(FoundDamaged {
        volume: listing.volume,
        number,
        file_length,
        head: None,
        columns,
    });
    Ok(())
}

/// Check a file has never taken a record: its first head is all zeros or absent
pub fn holds_nothing<File: IoFile>(file: &File) -> Result<bool> {
    let mut head = [0; SMALL_HEAD_BYTES];
    let read = file.read_at(FileOffset(0), &mut head)?;
    let mut is_zero = true;
    for byte in &head[..read.0 as usize] {
        is_zero &= *byte == 0;
    }
    Ok(is_zero)
}

impl<Backend: Io> EngineState<Backend> {
    /// Put a sealed segment in the table and the directories
    pub fn install_sealed(&self, volume: &Volume<Backend>, found: FoundSealed<Backend::File>) -> Result<()> {
        // a kill between a footer's sync and its rows file going leaves the rows behind
        match self.io.remove_file(&rows_path(&found.path, found.head.segment)) {
            Ok(()) => {},
            Err(error) if error.kind() == ErrorKind::NotFound => {},
            Err(error) => return Err(error),
        }

        volume.note_sealed(found.head.places.highest);
        if !volume.table.install(found.file, &found.head, &found.columns, &found.entries) {
            return Err(Error::other("segment table sized below what the roots hold"));
        }
        for column in &found.columns {
            let entry = DirectoryEntry {
                shared_length: column.shared_length,
                shared_sum: column.shared_sum,
                lowest: column.lowest,
                highest: column.highest,
                reach: column.highest,
                segment: found.head.segment,
            };
            if let Some(directory) = &self.directories[column.column.0 as usize]
                && !directory.replace(Some(&entry), &[])
            {
                return Err(Error::other("column directory sized below what the roots hold"));
            }
        }
        Ok(())
    }

    /// Rebuild an open segment from its rows, ready to go on taking writes, or return false
    /// with nothing changed when its rows don't fit the write index
    pub fn recover_open(&self, volume: &Volume<Backend>, scanned: &ScannedOpen<Backend::File>) -> Result<bool> {
        let Some(slot) = self.take_slot() else {
            return Err(Error::other("more open segments than slots"));
        };
        let found = &scanned.found;
        let mut recovered = RecoveredRows {
            write_end: FileOffset(0),
            row_count: RowCount(0),
            lowest_place: WritePlace(u64::MAX),
            highest_place: WritePlace(0),
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
        scanned.rows.truncate(ByteCount(kept_end.0))?;

        let synced_through = FileOffset(scanned.scan.synced_through.0.min(kept_end.0));
        hold(&self.queues[slot.0 as usize]).reset(kept_end, synced_through);
        self.slot_files[slot.0 as usize].set(Some(SlotData {
            segment: found.number,
            file: found.data.clone(),
        }));
        volume.open_slot(slot);
        hold(&volume.claims).open.push(OpenSegment {
            number: found.number,
            slot,
            root: found.root,
            data: found.data.clone(),
            rows: scanned.rows.clone(),
            phase: SegmentPhase::Taking,
            write_end: recovered.write_end,
            row_count: recovered.row_count,
            in_flight: 0,
            lowest_place: recovered.lowest_place,
            highest_place: recovered.highest_place,
        });
        Ok(true)
    }

    /// Walk a rows file's whole groups for where they end and what a sync last covered
    pub fn scan_rows(&self, rows: &Backend::File) -> Result<RowsScan> {
        let mut scan = RowsScan::default();
        let mut bytes = vec![0; GROUP_BYTES.0 as usize];
        let length = rows.length()?;

        while scan.valid_end.0 < length.0 {
            let Some(group_length) = read_group(rows, scan.valid_end, length, &mut bytes)? else {
                break;
            };
            let Some(group) = parse_group(&bytes[..group_length], self.record_key) else {
                break;
            };
            scan.synced_through = scan.synced_through.max(group.synced_through);
            scan.row_count = RowCount(scan.row_count.0 + group.row_count.0);
            scan.valid_end = FileOffset(scan.valid_end.0 + group_length as u64);
        }
        Ok(scan)
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
        let mut value = Vec::new();
        let mut at = FileOffset(0);
        let end = ByteCount(scan.valid_end.0);

        while at.0 < scan.valid_end.0 {
            let Some(length) = read_group(rows, at, end, &mut bytes)? else {
                break;
            };
            let Some(group) = parse_group(&bytes[..length], self.record_key) else {
                break;
            };

            // a group past the last sync may list a record a power cut lost, and it goes whole
            if at.0 >= scan.synced_through.0 {
                for row in group.walk() {
                    if !self.is_record_whole(target.data, &row, records, &mut value)? {
                        return Ok(Some(at));
                    }
                }
            }

            for row in group.walk() {
                if !self.replay_row(&row, target, recovered) {
                    return Ok(None);
                }
            }
            at = FileOffset(at.0 + length as u64);
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
        if row.kind != RowKind::Value {
            return Ok(true);
        }

        let identity = RecordIdentity {
            record_key: self.record_key,
            column: row.column,
            key: row.key,
            kind: row.kind,
        };
        let location = RecordLocation {
            offset: row.offset,
            length: row.length,
        };
        let Some(info) = self.columns[row.column.0 as usize] else {
            return Ok(false);
        };
        match records.read_value(data, location, &identity, info.codec, value) {
            Ok(RecordRead::Valid) => Ok(true),
            Ok(RecordRead::Failed) => Ok(false),
            Err(error) if error.kind() == ErrorKind::UnexpectedEof => Ok(false),
            Err(error) => Err(error),
        }
    }

    /// Put one recovered row in the write index, and return false when it has no room
    pub fn replay_row(
        &self,
        row: &RowEntry,
        target: &ReplayTarget<Backend>,
        recovered: &mut RecoveredRows,
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

        recovered.row_count = RowCount(recovered.row_count.0 + 1);
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

/// Bring every open segment back, sealing as a running store at this budget would have
pub fn recover_segments<Backend: Io>(
    state: &Arc<EngineState<Backend>>,
    found: Vec<FoundOpen<Backend::File>>,
    highest_segment: SegmentNumber,
) -> Result<()> {
    let mut scanned = Vec::with_capacity(found.len());
    for open in found {
        let volume = &state.volumes[open.volume];
        let path = &volume.roots[open.root.0 as usize];
        let rows = state.io.open_file(&rows_path(path, open.number))?;
        let scan = state.scan_rows(&rows)?;
        scanned.push(ScannedOpen { found: open, rows, scan });
    }
    state.resume_numbers(highest_segment);

    for segment in scanned {
        let index = segment.found.volume;
        let volume = &state.volumes[index];
        if segment.scan.row_count > volume.segment_rows {
            copy_segment(state, index, &segment)?;
            continue;
        }

        make_room(state, index, segment.scan.row_count)?;
        if state.recover_open(volume, &segment)? {
            continue;
        }

        // long keys can fill the index below the row limit
        seal_every_open(state, index)?;
        if !state.recover_open(volume, &segment)? {
            copy_segment(state, index, &segment)?;
        }
    }
    Ok(())
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

/// Copy a segment too large for this budget's index into new segments, keeping every place, then
/// drop it: a crash part way leaves it whole, and the copies only repeat rows it still holds
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

    'groups: while at.0 < segment.scan.valid_end.0 {
        let Some(length) = read_group(&segment.rows, at, end, &mut bytes)? else {
            break;
        };
        let Some(group) = parse_group(&bytes[..length], state.record_key) else {
            break;
        };

        if at.0 >= segment.scan.synced_through.0 {
            for row in group.walk() {
                if !state.is_record_whole(&segment.found.data, &row, &mut records, &mut value)? {
                    break 'groups;
                }
            }
        }
        for row in group.walk() {
            copy_row(state, volume, &segment.found.data, &row, &mut record)?;
        }
        at = FileOffset(at.0 + length as u64);
    }

    let target = &state.volumes[volume];
    state.sync_volume(target).map_err(open_failure)?;
    let path = &target.roots[segment.found.root.0 as usize];
    state.io.close_file(segment.found.data.clone());
    state.io.close_file(segment.rows.clone());
    state.io.remove_file(&data_path(path, segment.found.number))?;
    state.io.remove_file(&rows_path(path, segment.found.number))?;
    state.io.sync_directory(path)
}

/// Copy one row and its record as stored, sealing when the limits say a running store would
pub fn copy_row<Backend: Io>(
    state: &Arc<EngineState<Backend>>,
    volume: usize,
    data: &Backend::File,
    row: &RowEntry,
    record: &mut Vec<u8>,
) -> Result<()> {
    let target = &state.volumes[volume];
    record.resize(row.length.0 as usize, 0);
    if !record.is_empty() {
        data.read_exact_at(row.offset, record)?;
    }

    let request = ClaimRequest {
        bytes: row.length,
        rows: RowCount(1),
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
pub fn read_group<File: IoFile>(rows: &File, at: FileOffset, end: ByteCount, bytes: &mut [u8]) -> Result<Option<usize>> {
    let left = end.0.saturating_sub(at.0) as usize;
    if left < GROUP_HEADER_BYTES {
        return Ok(None);
    }
    rows.read_exact_at(at, &mut bytes[..GROUP_HEADER_BYTES])?;
    let Some(length) = read_half(bytes, TOTAL_LENGTH_AT) else {
        return Ok(None);
    };

    let length = length as usize;
    if length > left || length > bytes.len() || length < GROUP_HEADER_BYTES {
        return Ok(None);
    }
    rows.read_exact_at(at, &mut bytes[..length])?;
    Ok(Some(length))
}

/// Read each volume's summary, which open reads after the tails
pub fn read_summaries<Backend: Io>(
    io: &Backend,
    roots: &[(usize, Vec<PathBuf>)],
) -> Result<Vec<SummaryFound>> {
    let mut found = Vec::with_capacity(roots.len());
    for (_volume, paths) in roots {
        let summary_paths = SummaryPaths::new(&paths[0]);
        let mut rows = SummaryRows::new(ByteCount(0));
        // the superblock's limit keeps the write order, so open has no use for the summary's mark
        let _mark = read_summary(io, &summary_paths, &mut rows)?;
        found.push(SummaryFound {
            rows,
            paths: summary_paths,
        });
    }
    Ok(found)
}

/// Bring each volume's summary in line with what the tails say, and mark what's damaged
pub fn settle_summaries<Backend: Io>(
    state: &EngineState<Backend>,
    sealed: &[FoundSealed<Backend::File>],
    damaged: &[FoundDamaged],
) -> Result<()> {
    for (index, volume) in state.volumes.iter().enumerate() {
        let mut present = BTreeSet::new();
        for found in sealed {
            if found.volume == index {
                present.insert(found.head.segment);
            }
        }
        for found in damaged {
            if found.volume == index {
                present.insert(found.number);
            }
        }

        hold(&volume.summary).keep_where(|row| present.contains(&row.facts.segment));
    }

    let mut reader = FooterReader::new();
    for found in sealed {
        check_sealed_row(state, found, &mut reader)?;
    }
    for found in damaged {
        open_damaged(state, found);
    }
    for volume in &state.volumes {
        state.save_summary(volume)?;
    }
    Ok(())
}

/// Check a sealed segment's row against its tail, and build the row again when it fails
pub fn check_sealed_row<Backend: Io>(
    state: &EngineState<Backend>,
    found: &FoundSealed<Backend::File>,
    reader: &mut FooterReader,
) -> Result<()> {
    let volume = &state.volumes[found.volume];
    let facts = SegmentFacts {
        segment: found.head.segment,
        file_length: found.file.length()?,
        data_end: found.head.data_end,
        row_block_end: found.head.index_at,
        seal_mark: found.head.index_sum,
        places: found.head.places,
        is_damaged: false,
    };

    let mut rows = hold(&volume.summary);
    if let Some(row) = rows.get(facts.segment) {
        let is_trusted = SegmentFacts { is_damaged: false, ..row.facts } == facts;
        if is_trusted {
            // spans outlive the fault that made them, and only clear_damage ends them
            if row.facts.is_damaged {
                let ranges = row_ranges(&row, facts.segment, &mut Vec::new());
                for range in ranges {
                    volume.damage.add(range);
                }
                hold(&volume.damaged).push(facts.segment);
            }
            return Ok(());
        }
    }
    rows.remove(facts.segment);

    let mut keys = Vec::with_capacity(found.columns.len());
    for column in &found.columns {
        let first = column.first_block as usize;
        let lines = &found.entries[first..first + column.block_count.0 as usize];
        let mut lowest = Vec::new();
        let mut highest = Vec::new();
        if !reader.edge_keys(&found.file, lines, found.head.place_base, &mut lowest, &mut highest)? {
            // a block that can't be read gets the widest range, which can only ask too much of a read
            lowest.clear();
            highest = vec![u8::MAX; MAX_KEY_BYTES as usize];
        }
        keys.push((lowest, highest));
    }

    let columns = found.columns.iter().zip(keys.iter()).map(|(column, edges)| ColumnFacts {
        column: column.column,
        lowest: &edges.0,
        highest: &edges.1,
        row_count: column.row_count,
        grave_count: column.grave_count,
    });
    // a row that won't fit costs nothing until the segment is damaged, and then its columns count whole
    let _is_kept = rows.push(&facts, columns, [].into_iter());
    Ok(())
}

/// Turn a damaged segment's summary row, or what's left of it, into ranges reads must respect
pub fn open_damaged<Backend: Io>(state: &EngineState<Backend>, found: &FoundDamaged) {
    let volume = &state.volumes[found.volume];
    let mut ranges = Vec::new();
    let mut facts = SegmentFacts {
        segment: found.number,
        file_length: found.file_length,
        is_damaged: true,
        ..SegmentFacts::default()
    };
    let mut columns: Vec<OwnedColumn> = Vec::new();

    let rows = hold(&volume.summary);
    let trusted = rows.get(found.number).filter(|row| is_row_trusted(row, found));
    if let Some(row) = trusted {
        facts = row.facts;
        facts.is_damaged = true;
        ranges = row_ranges(&row, found.number, &mut columns);
    }
    let was_recorded = trusted.is_some();
    drop(rows);

    if !was_recorded {
        fallback_ranges(state, found, &mut facts, &mut ranges);
    }

    for range in &ranges {
        volume.damage.add(range.clone());
    }
    hold(&volume.damaged).push(found.number);
    rewrite_row(volume, &facts, &columns, &ranges);
}

/// Check a row can speak for a damaged segment: its length always, and the tail's facts when one reads
pub fn is_row_trusted(row: &SummaryRow, found: &FoundDamaged) -> bool {
    if row.facts.file_length != found.file_length {
        return false;
    }
    match &found.head {
        Some(head) => row.facts.seal_mark == head.index_sum && row.facts.places == head.places,
        None => true,
    }
}

/// Read the damaged ranges a row gives, and copy its columns out
pub fn row_ranges(row: &SummaryRow, segment: SegmentNumber, columns: &mut Vec<OwnedColumn>) -> Vec<DamagedRange> {
    let mut ranges = Vec::new();
    let highest_place = row.facts.places.highest;
    let mut cursor = row.cursor();
    for _column in 0..row.column_count {
        let Some(column) = cursor.column() else {
            break;
        };
        columns.push(OwnedColumn {
            column: column.column,
            lowest: column.lowest.to_vec(),
            highest: column.highest.to_vec(),
            row_count: column.row_count,
            grave_count: column.grave_count,
        });
    }

    if row.facts.is_damaged {
        // the spans a row keeps are all that's left, since a caller may have cleared the rest
        for _span in 0..row.span_count {
            let Some(span) = cursor.span() else {
                break;
            };
            ranges.push(DamagedRange {
                column: span.column,
                segment,
                highest_place,
                start: span.start.to_vec(),
                end: span.end.map(<[u8]>::to_vec),
            });
        }
        return ranges;
    }

    for column in columns.iter() {
        let mut end = column.highest.clone();
        end.push(0);
        ranges.push(DamagedRange {
            column: column.column,
            segment,
            highest_place,
            start: column.lowest.clone(),
            end: Some(end),
        });
    }
    ranges
}

/// Cover whole columns when no row can speak for a damaged segment
pub fn fallback_ranges<Backend: Io>(
    state: &EngineState<Backend>,
    found: &FoundDamaged,
    facts: &mut SegmentFacts,
    ranges: &mut Vec<DamagedRange>,
) {
    let highest_place = match &found.head {
        Some(head) => {
            facts.places = head.places;
            facts.data_end = head.data_end;
            facts.row_block_end = head.index_at;
            facts.seal_mark = head.index_sum;
            head.places.highest
        },
        None => {
            // every row a read can find sits at or below the counter at open, and every new write above it
            let counter = state.order.mark();
            facts.places.highest = counter;
            counter
        },
    };

    let mut add = |column: ColumnId| {
        ranges.push(DamagedRange {
            column,
            segment: found.number,
            highest_place,
            start: Vec::new(),
            end: None,
        });
    };
    if found.head.is_some() {
        for column in &found.columns {
            add(column.column);
        }
        return;
    }
    for (id, info) in state.columns.iter().enumerate() {
        if info.is_some_and(|info| info.volume.0 as usize == found.volume) {
            add(ColumnId(id as u8));
        }
    }
}

/// Replace a segment's row with one that keeps these spans
pub fn rewrite_row<Backend: Io>(
    volume: &Volume<Backend>,
    facts: &SegmentFacts,
    columns: &[OwnedColumn],
    ranges: &[DamagedRange],
) {
    let mut rows = hold(&volume.summary);
    rows.remove(facts.segment);
    let column_facts = columns.iter().map(|column| ColumnFacts {
        column: column.column,
        lowest: &column.lowest,
        highest: &column.highest,
        row_count: column.row_count,
        grave_count: column.grave_count,
    });
    let spans = ranges.iter().map(|range| SpanFacts {
        column: range.column,
        start: &range.start,
        end: range.end.as_deref(),
    });
    // a row that won't fit leaves the segment with no row, and open then covers its columns whole
    let _is_kept = rows.push(facts, column_facts, spans);
}

/// Record a cleared span in every damaged segment's row
pub fn save_cleared_spans<Backend: Io>(volume: &Volume<Backend>) {
    let numbers = hold(&volume.damaged).clone();
    for number in numbers {
        let ranges = volume.damage.of_segment(number);
        let mut columns = Vec::new();
        let rows = hold(&volume.summary);
        let Some(row) = rows.get(number) else {
            continue;
        };
        let facts = row.facts;
        let _ranges_in_row = row_ranges(&row, number, &mut columns);
        drop(rows);
        rewrite_row(volume, &facts, &columns, &ranges);
    }
}
