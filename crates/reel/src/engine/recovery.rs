//! Open's reading of the roots: sealed segments from their tails, open ones from their rows

use std::collections::{BTreeMap, BTreeSet};
use std::io::{Error, ErrorKind, IoSlice, Result};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use reel_api::column_id::ColumnId;
use reel_api::store_error::StoreError;
use reel_api::units::{ByteCount, FileOffset, RootId, RowCount, WritePlace};

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
use crate::rows::row_group::{GROUP_HEADER_BYTES, GROUP_MAGIC, RowEntry, TOTAL_LENGTH_AT, parse_group};
use crate::segment::block_entry::BlockEntry;
use crate::segment::column_directory::DirectoryEntry;
use crate::segment::damage_list::DamagedRange;
use crate::segment::footer_reader::FooterReader;
use crate::segment::segment_table::{TABLE_SHARDS, TableSizing, shard_of};
use crate::segment::segment_tail::{ColumnTail, SegmentNumber, TailHead};
use crate::segment::summary::{
    ColumnFacts, SUMMARY_KEY_BYTES, SegmentFacts, SpanFacts, SummaryPaths, SummaryRow, SummaryRows, put_bound, put_lowest,
    read_summary,
};

/// Steps a seal on the opening thread can take before open reports it stuck
pub const MOST_INLINE_STEPS: u64 = 100_000_000;
/// Bytes read at a time while searching a rows file for the group after a bad one
pub const GROUP_SEARCH_BYTES: usize = 4_096;

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

/// A data file that never took a record, unlinked unless the summary lists it
#[derive(Clone, Debug)]
pub struct FoundEmpty {
    pub volume: usize,
    pub root: PathBuf,
    pub number: SegmentNumber,
    pub file_length: ByteCount,
}

/// What one segment turned out to be
#[derive(Debug)]
pub enum SegmentRead {
    /// Its tail and index read
    Sealed {
        head: TailHead,
        columns: Vec<ColumnTail>,
        entries: Vec<BlockEntry>,
    },
    /// It has a rows file, so it was open when the store stopped
    Open,
    /// Its copies fail, and its file stays for the rebuild
    Damaged {
        head: Option<TailHead>,
        columns: Vec<ColumnTail>,
        file_length: ByteCount,
    },
    /// Its first head is empty and nothing else says what it is
    Empty {
        file_length: ByteCount,
    },
}

/// A column's facts with its keys copied out of the summary
#[derive(Clone, Debug)]
pub struct OwnedColumn {
    pub column: ColumnId,
    pub lowest: Vec<u8>,
    pub bound: Vec<u8>,
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
    pub empty: Vec<FoundEmpty>,
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
    pub has_rot: bool,
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
    /// Close every file the scan still holds
    pub fn close_files<Backend: Io<File = File>>(&mut self, io: &Backend) {
        for found in self.sealed.drain(..) {
            io.close_file(found.file);
        }
        for found in self.open.drain(..) {
            io.close_file(found.data);
        }
    }

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

/// Read every listed segment's tail and sort them into sealed, open and damaged, closing all on a failure
pub fn scan_roots<Backend: Io>(
    io: &Backend,
    record_key: RecordKey,
    listings: &[RootListing],
) -> Result<RootScan<Backend::File>> {
    let mut scan = RootScan {
        sealed: Vec::new(),
        open: Vec::new(),
        damaged: Vec::new(),
        empty: Vec::new(),
        highest_segment: SegmentNumber(0),
    };
    let mut reader = FooterReader::new();

    for listing in listings {
        for path in &listing.files {
            let Some(number) = parse_segment(path, DATA_SUFFIX) else {
                continue;
            };
            scan.highest_segment = scan.highest_segment.max(number);
            let sorted = io
                .open_file(path)
                .and_then(|file| sort_segment(io, record_key, listing, number, file, &mut reader, &mut scan));
            if let Err(error) = sorted {
                scan.close_files(io);
                return Err(error);
            }
        }
    }
    Ok(scan)
}

/// Read one segment and hand its file to the scan, or close it
pub fn sort_segment<Backend: Io>(
    io: &Backend,
    record_key: RecordKey,
    listing: &RootListing,
    number: SegmentNumber,
    file: Backend::File,
    reader: &mut FooterReader,
    scan: &mut RootScan<Backend::File>,
) -> Result<()> {
    let segment = match read_segment(record_key, listing, number, &file, reader) {
        Ok(segment) => segment,
        Err(error) => {
            io.close_file(file);
            return Err(error);
        },
    };

    match segment {
        SegmentRead::Sealed { head, columns, entries } => scan.sealed.push(FoundSealed {
            volume: listing.volume,
            path: listing.path.clone(),
            file,
            head,
            columns,
            entries,
        }),
        SegmentRead::Open => scan.open.push(FoundOpen {
            volume: listing.volume,
            root: listing.root,
            number,
            data: file,
        }),
        SegmentRead::Damaged { head, columns, file_length } => {
            io.close_file(file);
            scan.damaged.push(FoundDamaged {
                volume: listing.volume,
                number,
                file_length,
                head,
                columns,
            });
        },
        SegmentRead::Empty { file_length } => {
            io.close_file(file);
            // a crash between creating the two files of a segment leaves this, and open checks the summary first
            scan.empty.push(FoundEmpty {
                volume: listing.volume,
                root: listing.path.clone(),
                number,
                file_length,
            });
        },
    }
    Ok(())
}

/// Work out what one segment is from its tail, its index and its rows file
pub fn read_segment<File: IoFile>(
    record_key: RecordKey,
    listing: &RootListing,
    number: SegmentNumber,
    file: &File,
    reader: &mut FooterReader,
) -> Result<SegmentRead> {
    let mut columns = Vec::new();
    if let Some(head) = reader.read_tail(file, record_key, &mut columns)? {
        let mut entries = Vec::new();
        if reader.read_index(file, &head, &mut entries)? {
            return Ok(SegmentRead::Sealed { head, columns, entries });
        }
        // open decides what its ranges are once it has read the summary
        return Ok(SegmentRead::Damaged {
            head: Some(head),
            columns,
            file_length: file.length()?,
        });
    }

    if listing.files.contains(&rows_path(&listing.path, number)) {
        return Ok(SegmentRead::Open);
    }
    // a head that can't be read may hold anything, so it counts as something
    if holds_nothing(file).unwrap_or(false) {
        return Ok(SegmentRead::Empty {
            file_length: file.length()?,
        });
    }
    Ok(SegmentRead::Damaged {
        head: None,
        columns,
        file_length: file.length()?,
    })
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
    pub fn install_sealed(&self, volume: &Volume<Backend>, found: &FoundSealed<Backend::File>) -> Result<()> {
        // a kill between a footer's sync and its rows file going leaves the rows behind
        match self.io.remove_file(&rows_path(&found.path, found.head.segment)) {
            Ok(()) => {},
            Err(error) if error.kind() == ErrorKind::NotFound => {},
            Err(error) => return Err(error),
        }

        volume.note_sealed(found.head.places.highest);
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
        // the table takes the file last, so a failure before it leaves one owner, the scan
        if !volume.table.install(found.file.clone(), &found.head, &found.columns, &found.entries) {
            return Err(Error::other("segment table sized below what the roots hold"));
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
        let unsynced = ByteCount(kept_end.0 - synced_through.0);
        rewrite_range(&scanned.rows, synced_through, unsynced, &mut vec![0; GROUP_BYTES.0 as usize])?;
        hold(&self.queues[slot.0 as usize]).reset(kept_end, synced_through);
        // what open keeps, a read can see: this sync covers it on disk, so no crash or rot can take it back
        self.write_out_turn(slot, &scanned.rows, Some(&found.data))?;
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

    /// Find the next whole group: the one at this offset, or past a bad one the first that shows a sync
    /// covered it, which makes the bad one rot and never a torn end
    pub fn next_group(&self, rows: &Backend::File, at: FileOffset, end: ByteCount, bytes: &mut [u8]) -> Option<(FileOffset, usize)> {
        if let Some(length) = read_group(rows, at, end, bytes)
            && parse_group(&bytes[..length], self.record_key).is_some()
        {
            return Some((at, length));
        }
        covering_group(rows, at, end, bytes, self.record_key)
    }

    /// Walk a rows file's whole groups for where they end and what a sync last covered
    pub fn scan_rows(&self, rows: &Backend::File) -> Result<RowsScan> {
        let mut scan = RowsScan::default();
        let mut bytes = vec![0; GROUP_BYTES.0 as usize];
        let length = rows.length()?;

        while scan.valid_end.0 < length.0 {
            let Some((at, group_length)) = self.next_group(rows, scan.valid_end, length, &mut bytes) else {
                break;
            };
            let Some(group) = parse_group(&bytes[..group_length], self.record_key) else {
                break;
            };
            scan.has_rot |= at != scan.valid_end;
            scan.synced_through = scan.synced_through.max(group.synced_through);
            scan.row_count = RowCount(scan.row_count.0 + group.row_count.0);
            scan.valid_end = FileOffset(at.0 + group_length as u64);
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
        let mut copy = vec![0; GROUP_BYTES.0 as usize];
        let mut value = Vec::new();
        let mut at = FileOffset(0);
        let end = ByteCount(scan.valid_end.0);

        while at.0 < scan.valid_end.0 {
            let Some((start, length)) = self.next_group(rows, at, end, &mut bytes) else {
                break;
            };
            at = start;
            let Some(group) = parse_group(&bytes[..length], self.record_key) else {
                break;
            };

            // a group past the last sync may list a record a power cut lost, and it goes whole
            if at.0 >= scan.synced_through.0 {
                for row in group.walk() {
                    if !self.is_record_whole(target.data, &row, records, &mut value)? {
                        return Ok(Some(at));
                    }
                    rewrite_range(target.data, row.offset, row.length, &mut copy)?;
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
        // a row whose record can't be read has no whole write behind it, whatever the reason
        let read = records.read_value(data, location, &identity, info.codec, value);
        Ok(matches!(read, Ok(RecordRead::Valid)))
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

/// Write a range back over itself: a kernel forgets the writes a failed sync covered and keeps showing
/// them, so bytes no sync covered must be dirty again before a later sync can say they're on disk
pub fn rewrite_range<File: IoFile>(file: &File, start: FileOffset, length: ByteCount, copy: &mut [u8]) -> Result<()> {
    let mut at = start.0;
    let end = start.0 + length.0;
    while at < end {
        let step = copy.len().min((end - at) as usize);
        file.read_exact_at(FileOffset(at), &mut copy[..step])?;
        file.write_at(FileOffset(at), &[IoSlice::new(&copy[..step])])?;
        at += step as u64;
    }
    Ok(())
}

/// Bring every open segment back, sealing as a running store at this budget would have, and leave
/// in `found` only what still has its files open when something fails
pub fn recover_segments<Backend: Io>(
    state: &Arc<EngineState<Backend>>,
    found: &mut Vec<FoundOpen<Backend::File>>,
    highest_segment: SegmentNumber,
) -> Result<()> {
    let mut scanned = Vec::with_capacity(found.len());
    let recovered = recover_all(state, found, &mut scanned, highest_segment);
    if recovered.is_err() {
        for segment in scanned {
            state.io.close_file(segment.found.data);
            state.io.close_file(segment.rows);
        }
    }
    recovered
}

/// Walk every rows file, then recover each segment, moving it out of its list once its files have a new owner
pub fn recover_all<Backend: Io>(
    state: &Arc<EngineState<Backend>>,
    found: &mut Vec<FoundOpen<Backend::File>>,
    scanned: &mut Vec<ScannedOpen<Backend::File>>,
    highest_segment: SegmentNumber,
) -> Result<()> {
    found.reverse();
    while let Some(open) = found.last() {
        let volume = &state.volumes[open.volume];
        let path = &volume.roots[open.root.0 as usize];
        let rows = state.io.open_file(&rows_path(path, open.number))?;
        let scan = match state.scan_rows(&rows) {
            Ok(scan) => scan,
            Err(error) => {
                state.io.close_file(rows);
                return Err(error);
            },
        };
        if scan.has_rot
            && let Err(error) = damage_open(state, open)
        {
            state.io.close_file(rows);
            return Err(error);
        };
        if let Some(open) = found.pop() {
            scanned.push(ScannedOpen { found: open, rows, scan });
        }
    }
    state.resume_numbers(highest_segment);

    scanned.reverse();
    while let Some(segment) = scanned.last() {
        let is_kept = recover_one(state, segment)?;
        if let Some(segment) = scanned.pop()
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
    if segment.scan.row_count > volume.segment_rows {
        copy_segment(state, index, segment)?;
        return Ok(false);
    }

    make_room(state, index, segment.scan.row_count)?;
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
    state.io.remove_file(&data_path(path, number))?;
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

    'groups: while at.0 < segment.scan.valid_end.0 {
        let Some((start, length)) = state.next_group(&segment.rows, at, end, &mut bytes) else {
            break;
        };
        at = start;
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
    state.sync_volume(target).map_err(open_failure)
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
pub fn read_group<File: IoFile>(rows: &File, at: FileOffset, end: ByteCount, bytes: &mut [u8]) -> Option<usize> {
    let left = end.0.saturating_sub(at.0) as usize;
    if left < GROUP_HEADER_BYTES {
        return None;
    }
    rows.read_exact_at(at, &mut bytes[..GROUP_HEADER_BYTES]).ok()?;
    let length = read_half(bytes, TOTAL_LENGTH_AT)? as usize;
    if length > left || length > bytes.len() || length < GROUP_HEADER_BYTES {
        return None;
    }
    rows.read_exact_at(at, &mut bytes[..length]).ok()?;
    Some(length)
}

/// Search past a bad group for a whole group whose sync mark reaches over it
pub fn covering_group<File: IoFile>(
    rows: &File,
    bad_at: FileOffset,
    end: ByteCount,
    bytes: &mut [u8],
    record_key: RecordKey,
) -> Option<(FileOffset, usize)> {
    let mut chunk = [0; GROUP_SEARCH_BYTES];
    let mut at = bad_at.0 + 1;
    while at + size_of::<u32>() as u64 <= end.0 {
        let length = GROUP_SEARCH_BYTES.min((end.0 - at) as usize);
        // a bad sector hides what it holds, and the search goes on after it
        if rows.read_exact_at(FileOffset(at), &mut chunk[..length]).is_ok() {
            for position in 0..=length - size_of::<u32>() {
                if read_half(&chunk, position) != Some(GROUP_MAGIC) {
                    continue;
                }
                let candidate = FileOffset(at + position as u64);
                if let Some(group_length) = read_group(rows, candidate, end, bytes)
                    && let Some(group) = parse_group(&bytes[..group_length], record_key)
                    && group.synced_through > bad_at
                {
                    return Some((candidate, group_length));
                }
            }
        }
        // the last bytes of a chunk go round again, since a marker can straddle two chunks
        at += (length - size_of::<u32>() + 1) as u64;
    }
    None
}

/// Mark an open segment's columns damaged whole when rot took a group a sync had covered, ranked at the
/// counter at open: the group's keys can't be known, and the mark stays through opens in the summary
pub fn damage_open<Backend: Io>(state: &EngineState<Backend>, found: &FoundOpen<Backend::File>) -> Result<()> {
    let volume = &state.volumes[found.volume];
    if hold(&volume.summary).get(found.number).is_some() {
        return Ok(());
    }

    let counter = state.order.mark();
    let facts = SegmentFacts {
        segment: found.number,
        is_damaged: true,
        damage_place: counter,
        ..SegmentFacts::default()
    };
    let mut ranges = Vec::new();
    for (id, info) in state.columns.iter().enumerate() {
        if info.is_some_and(|info| info.volume.0 as usize == found.volume) {
            ranges.push(DamagedRange {
                column: ColumnId(id as u8),
                segment: found.number,
                highest_place: counter,
                start: Vec::new(),
                end: None,
            });
        }
    }

    for range in &ranges {
        volume.damage.add(range.clone());
    }
    hold(&volume.damaged).push(found.number);
    if !push_row(&mut hold(&volume.summary), &facts, &[], &ranges) {
        state.note_dropped_row();
    }
    state.save_summary(volume)
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

/// Bring each volume's summary in line with what open found, in one walk of its rows
pub fn settle_summaries<Backend: Io>(state: &EngineState<Backend>, scan: &mut RootScan<Backend::File>) -> Result<()> {
    let mut reader = FooterReader::new();
    for (index, volume) in state.volumes.iter().enumerate() {
        let mut rows = hold(&volume.summary);
        let listed = rows.offsets();
        sort_empties(state, scan, index, &listed)?;

        // rows of segments no root holds stay behind here
        let mut settled = SummaryRows::new(ByteCount(0));
        settled.make_room(rows.heap_bytes());
        let mut held = BTreeSet::new();
        for found in &scan.sealed {
            if found.volume == index {
                held.insert(found.head.segment);
                settle_sealed(state, volume, found, &rows, &listed, &mut settled, &mut reader)?;
            }
        }
        for found in &scan.damaged {
            if found.volume == index {
                held.insert(found.number);
                let row = listed.get(&found.number).and_then(|at| rows.row_at(*at));
                settle_damaged(state, volume, found, row, &mut settled);
            }
        }
        let open: BTreeSet<SegmentNumber> =
            scan.open.iter().filter(|found| found.volume == index).map(|found| found.number).collect();
        carry_rows(state, volume, &rows, &listed, (&held, &open), &mut settled);
        *rows = settled;
        drop(rows);
        state.save_summary(volume)?;
    }
    Ok(())
}

/// Keep the rows no sealed or damaged segment took: an open segment's, and a gone segment's spans, which
/// stay until the caller clears them
pub fn carry_rows<Backend: Io>(
    state: &EngineState<Backend>,
    volume: &Volume<Backend>,
    rows: &SummaryRows,
    listed: &BTreeMap<SegmentNumber, usize>,
    (held, open): (&BTreeSet<SegmentNumber>, &BTreeSet<SegmentNumber>),
    settled: &mut SummaryRows,
) {
    for (number, at) in listed {
        let Some(row) = rows.row_at(*at) else {
            continue;
        };
        if held.contains(number) || !row.facts.is_damaged || (!open.contains(number) && row.span_count == 0) {
            continue;
        }

        for range in row_ranges(&row, *number, &mut Vec::new()) {
            volume.damage.add(range);
        }
        hold(&volume.damaged).push(*number);
        if !settled.copy_row(rows, *at) {
            state.note_dropped_row();
        }
    }
}

/// Unlink the files that never took a record, and treat one the summary lists as damaged
pub fn sort_empties<Backend: Io>(
    state: &EngineState<Backend>,
    scan: &mut RootScan<Backend::File>,
    volume: usize,
    listed: &BTreeMap<SegmentNumber, usize>,
) -> Result<()> {
    let mut index = 0;
    while index < scan.empty.len() {
        if scan.empty[index].volume != volume {
            index += 1;
            continue;
        }
        let found = scan.empty.swap_remove(index);
        if listed.contains_key(&found.number) {
            // a zeroed first sector on a sealed segment reads like this, and the row says what it is
            scan.damaged.push(FoundDamaged {
                volume,
                number: found.number,
                file_length: found.file_length,
                head: None,
                columns: Vec::new(),
            });
            continue;
        }
        state.io.remove_file(&data_path(&found.root, found.number))?;
    }
    Ok(())
}

/// Copy a sealed segment's row when it matches the tail, and build it again when it doesn't
pub fn settle_sealed<Backend: Io>(
    state: &EngineState<Backend>,
    volume: &Volume<Backend>,
    found: &FoundSealed<Backend::File>,
    rows: &SummaryRows,
    listed: &BTreeMap<SegmentNumber, usize>,
    settled: &mut SummaryRows,
    reader: &mut FooterReader,
) -> Result<()> {
    let facts = SegmentFacts {
        segment: found.head.segment,
        file_length: found.file.length()?,
        data_end: found.head.data_end,
        row_block_end: found.head.index_at,
        seal_mark: found.head.index_sum,
        places: found.head.places,
        is_damaged: false,
        damage_place: WritePlace(0),
    };

    if let Some(at) = listed.get(&facts.segment)
        && let Some(row) = rows.row_at(*at)
        && is_same_segment(&row.facts, &facts)
    {
        // spans outlive the fault that made them, and only clear_damage ends them
        if row.facts.is_damaged {
            for range in row_ranges(&row, facts.segment, &mut Vec::new()) {
                volume.damage.add(range);
            }
            hold(&volume.damaged).push(facts.segment);
        }
        if !settled.copy_row(rows, *at) {
            state.note_dropped_row();
        }
        return Ok(());
    }

    let mut keys = Vec::with_capacity(found.columns.len());
    for column in &found.columns {
        let first = column.first_block as usize;
        let lines = found.entries.get(first..first + column.block_count.0 as usize).unwrap_or_default();
        let mut lowest = Vec::new();
        let mut highest = Vec::new();
        if !reader.edge_keys(&found.file, lines, found.head.place_base, &mut lowest, &mut highest)? {
            // a block that can't be read gets the widest range, which can only ask too much of a read
            keys.push((Vec::new(), Vec::new()));
            continue;
        }
        let mut kept_lowest = Vec::with_capacity(SUMMARY_KEY_BYTES);
        let mut bound = Vec::with_capacity(SUMMARY_KEY_BYTES + 1);
        put_lowest(&lowest, &mut kept_lowest);
        put_bound(&highest, &mut bound);
        keys.push((kept_lowest, bound));
    }

    let columns = found.columns.iter().zip(keys.iter()).map(|(column, edges)| ColumnFacts {
        column: column.column,
        lowest: &edges.0,
        bound: &edges.1,
        row_count: column.row_count,
        grave_count: column.grave_count,
    });
    let mut ranges = Vec::new();
    let mut facts = facts;
    // a seal that crashed before its row went in leaves the open segment's row, and its spans go on
    if let Some(row) = listed.get(&facts.segment).and_then(|at| rows.row_at(*at))
        && is_open_row(&row)
    {
        ranges = row_ranges(&row, facts.segment, &mut Vec::new());
        facts.is_damaged = true;
        facts.damage_place = row.facts.damage_place;
        for range in &ranges {
            volume.damage.add(range.clone());
        }
        hold(&volume.damaged).push(facts.segment);
    }
    if !settled.push(&facts, columns, ranges.iter().map(span_of)) {
        state.note_dropped_row();
    }
    Ok(())
}

/// Turn a damaged segment's summary row, or what's left of it, into ranges reads must respect
pub fn settle_damaged<Backend: Io>(
    state: &EngineState<Backend>,
    volume: &Volume<Backend>,
    found: &FoundDamaged,
    row: Option<SummaryRow>,
    settled: &mut SummaryRows,
) {
    let mut ranges = Vec::new();
    let mut facts = SegmentFacts {
        segment: found.number,
        file_length: found.file_length,
        is_damaged: true,
        ..SegmentFacts::default()
    };
    let mut columns: Vec<OwnedColumn> = Vec::new();

    match row.filter(|row| is_row_trusted(row, found)) {
        Some(row) => {
            facts = row.facts;
            facts.is_damaged = true;
            if !row.facts.is_damaged {
                facts.damage_place = row.facts.places.highest;
            }
            ranges = row_ranges(&row, found.number, &mut columns);
        },
        None => fallback_ranges(state, found, &mut facts, &mut ranges),
    }

    for range in &ranges {
        volume.damage.add(range.clone());
    }
    hold(&volume.damaged).push(found.number);
    if !push_row(settled, &facts, &columns, &ranges) {
        state.note_dropped_row();
    }
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
    let highest_place = match row.facts.is_damaged {
        true => row.facts.damage_place,
        false => row.facts.places.highest,
    };
    let mut cursor = row.cursor();
    for _column in 0..row.column_count {
        let Some(column) = cursor.column() else {
            break;
        };
        columns.push(OwnedColumn {
            column: column.column,
            lowest: column.lowest.to_vec(),
            bound: column.bound.to_vec(),
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
        ranges.push(DamagedRange {
            column: column.column,
            segment,
            highest_place,
            start: column.lowest.clone(),
            end: (!column.bound.is_empty()).then(|| column.bound.clone()),
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
            facts.damage_place = head.places.highest;
            head.places.highest
        },
        None => {
            // every row a read can find sits at or below the counter at open, and every new write above it
            let counter = state.order.mark();
            facts.places.highest = counter;
            facts.damage_place = counter;
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

/// Check a row and a tail describe the same segment, whatever its row says about damage
pub fn is_same_segment(row: &SegmentFacts, tail: &SegmentFacts) -> bool {
    let without_damage = SegmentFacts {
        is_damaged: false,
        damage_place: WritePlace(0),
        ..*row
    };
    without_damage == *tail
}

/// Check a row was written for a segment still open, which has no length to speak of
pub fn is_open_row(row: &SummaryRow) -> bool {
    row.facts.is_damaged && row.facts.file_length.0 == 0
}

/// Read a range as the span a row keeps
pub fn span_of(range: &DamagedRange) -> SpanFacts<'_> {
    SpanFacts {
        column: range.column,
        start: &range.start,
        end: range.end.as_deref(),
    }
}

/// Add a row that keeps these spans, or return false when it doesn't fit
pub fn push_row(rows: &mut SummaryRows, facts: &SegmentFacts, columns: &[OwnedColumn], ranges: &[DamagedRange]) -> bool {
    let column_facts = columns.iter().map(|column| ColumnFacts {
        column: column.column,
        lowest: &column.lowest,
        bound: &column.bound,
        row_count: column.row_count,
        grave_count: column.grave_count,
    });
    rows.push(facts, column_facts, ranges.iter().map(span_of))
}

/// Write what's left of every damaged segment's spans into its row, or return false when a row is
/// missing or no longer fits, which leaves the rows as they were
pub fn save_cleared_spans<Backend: Io>(volume: &Volume<Backend>) -> bool {
    let numbers = hold(&volume.damaged).clone();
    let mut rows = hold(&volume.summary);
    let mut rewritten = SummaryRows::new(ByteCount(0));
    rewritten.make_room(rows.heap_bytes());

    let listed = rows.offsets();
    for (segment, at) in &listed {
        if numbers.contains(segment) {
            continue;
        }
        if !rewritten.copy_row(&rows, *at) {
            return false;
        }
    }
    for number in numbers {
        let Some(row) = listed.get(&number).and_then(|at| rows.row_at(*at)) else {
            return false;
        };
        let mut columns = Vec::new();
        let _ranges_in_row = row_ranges(&row, number, &mut columns);
        if !push_row(&mut rewritten, &row.facts, &columns, &volume.damage.of_segment(number)) {
            return false;
        }
    }
    *rows = rewritten;
    true
}
