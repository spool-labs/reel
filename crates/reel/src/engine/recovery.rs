//! Open's reading of the roots: sealed segments from their tails, open ones from their rows

use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet};
use std::io::{Error, ErrorKind, IoSlice, Result};
use std::mem;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering;

use reel_api::column_id::ColumnId;
use reel_api::store_error::StoreError;
use reel_api::units::{ByteCount, FileOffset, RootId, Nanoseconds, RowCount, WritePlace};
use xxhash_rust::xxh3::Xxh3;

use crate::engine::engine_sizing::GROUP_BYTES;
use crate::engine::engine_state::{EngineState, SlotData};
use crate::engine::open_segment::{DATA_SUFFIX, OUTPUT_SUFFIX, ROWS_SUFFIX, OpenSegment, SegmentPhase, data_path, parse_segment, rows_path};
use crate::engine::seal_job::SealJob;
use crate::engine::volume::{MOST_PARKED, Volume};
use crate::engine::write_path::{ClaimRequest, LandOutcome};
use crate::guard::hold;
use crate::index::open_slots::OpenSlot;
use crate::index::write_row::WriteRow;
use crate::lifetime::MarkRange;
use crate::io::job::{Job, JobStep};
use crate::io::device_error::{SECTOR_BYTES, is_device_error, read_or_damaged};
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
use crate::segment::block_entry::BlockEntry;
use crate::segment::column_directory::Listing;
use crate::segment::damage_list::{DamagedRange, highest_of, spans_of};
use crate::segment::range_covers::{Cover, RangeCovers, read_covers};
use crate::segment::footer_reader::FooterReader;
use crate::segment::segment_table::{TABLE_SHARDS, TableSizing, shard_of};
use crate::segment::segment_tail::{ColumnTail, SegmentNumber, TailHead, columns_fit};
use crate::segment::summary::{
    ColumnFacts, LostRows, MOST_ROW_COLUMN_BYTES, MOST_ROW_FIXED_BYTES, SUMMARY_KEY_BYTES, SegmentFacts, SpanFacts,
    SummaryPaths,
    SummaryRow, SummaryRows, put_bound, put_lowest,
    read_summary,
};

/// Steps a seal on the opening thread can take before open reports it stuck
pub const MOST_INLINE_STEPS: u64 = 100_000_000;
/// Bytes of a sector, the most a bad spot of a disk hides at once
pub const GROUP_SEARCH_BYTES: usize = SECTOR_BYTES;

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
    pub has_rows: bool,
}

/// Which segment file an open is sorting, and how
#[derive(Clone, Copy, Debug)]
pub struct SegmentAt<'listing> {
    pub record_key: RecordKey,
    pub listing: &'listing RootListing,
    pub number: SegmentNumber,
    pub mode: OpenMode,
}

/// Whether an open owns the store or only looks at it
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OpenMode {
    /// It holds the lock and writes
    Writable,
    /// It reads beside a writer, and changes nothing
    ReadOnly,
}

/// A sealed segment whose copies of the tail or the index all fail
#[derive(Clone, Debug)]
pub struct FoundDamaged {
    pub volume: usize,
    pub path: PathBuf,
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
    pub dead_rows: RowCount,
    pub dead_graves: RowCount,
    pub live_bytes: ByteCount,
    pub dead_bytes: ByteCount,
}

/// A volume's summary as open found it
#[derive(Debug)]
pub struct SummaryFound {
    pub rows: SummaryRows,
    pub paths: SummaryPaths,
    pub lost: LostRows,
}

/// A segment with no valid tail and a rows file
#[derive(Debug)]
pub struct FoundOpen<File> {
    pub volume: usize,
    pub root: RootId,
    pub number: SegmentNumber,
    pub data: File,
}

/// A file open couldn't remove, which the park holds until a retry gets it gone
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HeldFile {
    pub volume: usize,
    pub number: SegmentNumber,
}

/// What a read of every root found
#[derive(Debug)]
pub struct RootScan<File> {
    pub sealed: Vec<FoundSealed<File>>,
    pub scanned: Vec<ScannedOpen<File>>,
    pub damaged: Vec<FoundDamaged>,
    pub empty: Vec<FoundEmpty>,
    pub highest_segment: SegmentNumber,
    pub held: Vec<HeldFile>,
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
    pub rot_sum: Checksum,
    pub highest_place: WritePlace,
}

/// The rows of a recovered segment so far
#[derive(Clone, Copy, Debug)]
pub struct RecoveredRows {
    pub write_end: FileOffset,
    pub row_count: RowCount,
    pub lowest_place: WritePlace,
    pub highest_place: WritePlace,
    pub lost_region: Option<(FileOffset, FileOffset)>,
    pub cover_rows: usize,
    pub marks: MarkRange,
    pub has_deletes: bool,
}

impl<File> RootScan<File> {
    /// Close every file the scan still holds
    pub fn close_files<Backend: Io<File = File>>(&mut self, io: &Backend) {
        for found in self.sealed.drain(..) {
            io.close_file(found.file);
        }
        for segment in self.scanned.drain(..) {
            io.close_file(segment.found.data);
            io.close_file(segment.rows);
        }
    }

    /// Work out the room the rows of what was found can need, at most
    pub fn least_summary_room(&self, volume: usize, columns: usize) -> usize {
        let sealed_columns: usize = self.sealed.iter().filter(|found| found.volume == volume).map(|found| found.columns.len()).sum();
        let sealed = self.sealed.iter().filter(|found| found.volume == volume).count();
        let others = self.damaged.iter().filter(|found| found.volume == volume).count()
            + self.scanned.iter().filter(|segment| segment.found.volume == volume).count()
            + self.empty.iter().filter(|found| found.volume == volume).count();
        let sealed_blocks: usize = self.sealed.iter().filter(|found| found.volume == volume).map(|found| found.entries.len()).sum();
        let fixed = (sealed + others) * MOST_ROW_FIXED_BYTES + sealed_blocks * size_of::<u32>();
        fixed + (sealed_columns + others * columns) * MOST_ROW_COLUMN_BYTES
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
    mode: OpenMode,
) -> Result<RootScan<Backend::File>> {
    let mut scan = RootScan {
        sealed: Vec::new(),
        scanned: Vec::new(),
        damaged: Vec::new(),
        empty: Vec::new(),
        highest_segment: SegmentNumber(0),
        held: Vec::new(),
    };
    let mut reader = FooterReader::new();

    for listing in listings {
        for path in &listing.files {
            // a stopped rewrite's output has no tail, and everything in it is still in its input
            if parse_segment(path, OUTPUT_SUFFIX).is_some() {
                if mode == OpenMode::Writable {
                    let _removed = io.remove_file(path);
                }
                continue;
            }
            let Some(number) = parse_segment(path, DATA_SUFFIX) else {
                continue;
            };
            scan.highest_segment = scan.highest_segment.max(number);
            let sorted = open_for(io, path, mode)
                .and_then(|file| sort_segment(io, &SegmentAt { record_key, listing, number, mode }, file, &mut reader, &mut scan));
            if let Err(error) = sorted {
                scan.close_files(io);
                return Err(error);
            }
        }
    }
    if mode == OpenMode::Writable {
        drop_inputs(io, &mut scan);
    }
    Ok(scan)
}

/// Remove a file that has to go, or note it for the park: a store that can't open is an outage for every key, and one
/// leftover file is not
pub fn remove_or_hold<Backend: Io>(io: &Backend, root: &Path, volume: usize, number: SegmentNumber, held: &mut Vec<HeldFile>) {
    let removed = match io.remove_file(&data_path(root, number)) {
        Ok(()) => true,
        Err(error) => error.kind() == ErrorKind::NotFound,
    };
    if !removed || io.sync_directory(root).is_err() {
        held.push(HeldFile { volume, number });
    }
}

/// Remove every segment a whole data output lists, before anything counts them, since the output has all that still answers.
/// A grave output whose inputs are still here belongs to a merge that never finished, and it goes instead
pub fn drop_inputs<Backend: Io>(io: &Backend, scan: &mut RootScan<Backend::File>) {
    let is_final = |found: &&FoundSealed<Backend::File>| !found.head.is_partial;
    let inputs: Vec<SegmentNumber> = scan.sealed.iter().filter(is_final).flat_map(|found| found.head.inputs()).copied().collect();
    for input in inputs {
        if let Some(at) = scan.sealed.iter().position(|found| found.head.segment == input) {
            let found = scan.sealed.remove(at);
            io.close_file(found.file);
            remove_or_hold(io, &found.path, found.volume, input, &mut scan.held);
        }
        if let Some(at) = scan.empty.iter().position(|found| found.number == input) {
            let found = scan.empty.remove(at);
            remove_or_hold(io, &found.root, found.volume, input, &mut scan.held);
        }
        if let Some(at) = scan.damaged.iter().position(|found| found.number == input) {
            let found = scan.damaged.remove(at);
            remove_or_hold(io, &found.path, found.volume, input, &mut scan.held);
        }
    }
    let mut index = 0;
    while index < scan.sealed.len() {
        let head = &scan.sealed[index].head;
        let is_held = |number: &SegmentNumber| scan.sealed.iter().any(|found| found.head.segment == *number);
        if !head.is_partial || !head.inputs().iter().any(is_held) {
            index += 1;
            continue;
        }
        let found = scan.sealed.remove(index);
        let number = found.head.segment;
        io.close_file(found.file);
        remove_or_hold(io, &found.path, found.volume, number, &mut scan.held);
    }
}

/// Read one segment and hand its files to the scan, or close them
pub fn sort_segment<Backend: Io>(
    io: &Backend,
    at: &SegmentAt,
    file: Backend::File,
    reader: &mut FooterReader,
    scan: &mut RootScan<Backend::File>,
) -> Result<()> {
    let SegmentAt { record_key, listing, number, .. } = *at;
    // a seal writes its tail before it takes the rows file, so a view that opens the rows first misses neither
    let mut opened = match walk_listed_rows(io, at) {
        Ok(opened) => opened,
        Err(error) => {
            io.close_file(file);
            return Err(error);
        },
    };
    let segment = match read_segment(record_key, listing, number, &file, reader) {
        Ok(segment) => segment,
        Err(error) => {
            io.close_file(file);
            if let Some((rows, _walked)) = opened {
                io.close_file(rows);
            }
            return Err(error);
        },
    };

    let tail = match &segment {
        SegmentRead::Sealed { head, .. } => Some(*head),
        SegmentRead::Damaged { head, .. } => *head,
        SegmentRead::Open | SegmentRead::Empty { .. } => None,
    };
    let is_stale = match (&tail, &opened) {
        (Some(head), Some((_rows, walked))) => is_tail_stale(head, walked),
        (_, _) => false,
    };
    let segment = if is_stale { SegmentRead::Open } else { segment };

    match segment {
        SegmentRead::Sealed { head, columns, entries } => scan.sealed.push(FoundSealed {
            volume: listing.volume,
            path: listing.path.clone(),
            file,
            head,
            columns,
            entries,
            has_rows: opened.is_some(),
        }),
        SegmentRead::Open => {
            let found = FoundOpen {
                volume: listing.volume,
                root: listing.root,
                number,
                data: file,
            };
            let Some((rows, walked)) = opened.take() else {
                // a view listed a rows file that went with no tail left behind, so the segment went too
                io.close_file(found.data);
                return Err(Error::from(ErrorKind::NotFound));
            };
            scan.scanned.push(ScannedOpen { found, rows, scan: walked });
        },
        SegmentRead::Damaged { head, columns, file_length } => {
            io.close_file(file);
            scan.damaged.push(FoundDamaged {
                volume: listing.volume,
                path: listing.path.clone(),
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
    if let Some((rows, _walked)) = opened {
        io.close_file(rows);
    }
    Ok(())
}

/// Check a segment has a rows file, in a listing that comes sorted
pub fn has_rows(listing: &RootListing, number: SegmentNumber) -> bool {
    let rows = rows_path(&listing.path, number);
    listing.files.binary_search_by(|file| file.as_path().cmp(&rows)).is_ok()
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
        if reader.read_index(file, &head, &mut entries)? && columns_fit(&columns, entries.len()) {
            return Ok(SegmentRead::Sealed { head, columns, entries });
        }
        // open decides what its ranges are once it has read the summary
        return Ok(SegmentRead::Damaged {
            head: Some(head),
            columns,
            file_length: file.length()?,
        });
    }

    if has_rows(listing, number) {
        return Ok(SegmentRead::Open);
    }
    // a head that can't be read may hold anything, so it counts as something
    let is_empty = match holds_nothing(file) {
        Ok(is_empty) => is_empty,
        Err(error) if is_device_error(&error) => false,
        Err(error) => return Err(error),
    };
    if is_empty {
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
    pub fn is_empty_past_the_park(&self, path: &Path, parked: usize) -> Result<bool> {
        if parked < MOST_PARKED {
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
                if !is_held && !self.is_empty_past_the_park(&path, parked.iter().flatten().count())? {
                    unlisted.push(number);
                }
            }
        }
        Ok(unlisted)
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
            delete_seen: None,
        });
        Ok(true)
    }

    /// Walk a rows file's whole groups for where they end and what a sync last covered
    pub fn scan_rows(&self, rows: &Backend::File) -> Result<RowsScan> {
        scan_rows_of(rows, self.record_key)
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
        state.order.resume_past(segment.scan.highest_place);
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
                        for range in whole_columns(state, volume, copied.segment, state.order.mark()) {
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

/// Mark an open segment's columns damaged whole when rot took a group a sync had covered, ranked at the
/// counter at open: the group's keys can't be known. The row keeps where the rot was, in its seal mark, so
/// the rot an earlier open ranked keeps its rank and clears, and new rot takes this open's counter
pub fn damage_open<Backend: Io>(
    state: &EngineState<Backend>,
    found: &FoundOpen<Backend::File>,
    rot_sum: Checksum,
) -> Result<()> {
    let volume = &state.volumes[found.volume];
    let is_known = hold(&volume.summary).get(found.number).is_some_and(|row| row.facts.seal_mark == rot_sum);
    if is_known {
        return Ok(());
    }

    let counter = state.order.mark();
    let facts = SegmentFacts {
        segment: found.number,
        seal_mark: rot_sum,
        is_damaged: true,
        has_lost_rows: true,
        damage_place: counter,
        ..SegmentFacts::default()
    };
    let ranges = whole_columns(state, found.volume, found.number, counter);

    volume.damage.drop_segment(found.number);
    for range in &ranges {
        volume.damage.add(range);
    }
    let mut damaged = hold(&volume.damaged);
    if !damaged.contains(&found.number) {
        damaged.push(found.number);
    }
    drop(damaged);
    let mut rows = hold(&volume.summary);
    rows.remove(found.number);
    let dropped = rows.push_kept(&facts, [].into_iter(), ranges.iter().map(span_of));
    drop(rows);
    state.note_dropped_rows(dropped);
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
        let mut lost = LostRows::default();
        let _mark = read_summary(io, &summary_paths, &mut rows, &mut lost)?;
        found.push(SummaryFound {
            rows,
            paths: summary_paths,
            lost,
        });
    }
    Ok(found)
}

/// Bring each volume's summary in line with what open found, in one walk of its rows
pub fn settle_summaries<Backend: Io>(state: &EngineState<Backend>, scan: &mut RootScan<Backend::File>) -> Result<()> {
    let mut reader = FooterReader::new();
    for (index, volume) in state.volumes.iter().enumerate() {
        let mut rows = hold(&volume.summary);
        mark_lost_rows(state, scan, index, &mut rows);
        let listed = rows.offsets();
        sort_empties(state, scan, index, &listed)?;

        // rows of segments no root holds stay behind here
        let mut settled = SummaryRows::new(ByteCount(0));
        settled.make_room(rows.byte_room());
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
            scan.scanned.iter().filter(|segment| segment.found.volume == index).map(|segment| segment.found.number).collect();
        carry_rows(volume, &rows, &listed, (&held, &open), &mut settled);
        settled.highest_forward = held.last().map_or(SegmentNumber(0), |highest| rows.highest_forward.min(*highest));
        let index_room = rows.index_room();
        *rows = settled;
        rows.make_index(index_room);
        drop(rows);
        state.save_summary(volume)?;
    }
    Ok(())
}

/// Keep the rows no sealed or damaged segment took: an open segment's, and a gone segment's spans, which
/// stay until the caller clears them
pub fn carry_rows<Backend: Io>(
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
            volume.damage.add(&range);
        }
        hold(&volume.damaged).push(*number);
        // the room was sized at open from every row it found, and a row with spans has to stay
        settled.make_room_for(rows, *at);
        let is_copied = settled.copy_row(rows, *at);
        debug_assert!(is_copied);
    }
}

/// Build a whole-column range for every column of a volume in a segment
pub fn whole_columns<Backend: Io>(
    state: &EngineState<Backend>,
    volume: usize,
    segment: SegmentNumber,
    counter: WritePlace,
) -> Vec<DamagedRange> {
    let mut ranges = Vec::new();
    for (id, info) in state.columns.iter().enumerate() {
        if info.is_some_and(|info| info.volume.0 as usize == volume) {
            ranges.push(DamagedRange {
                column: ColumnId(id as u8),
                segment,
                highest_place: counter,
                start: Vec::new(),
                end: None,
            });
        }
    }
    ranges
}

/// Damage a segment's columns whole at the counter, keeping its row's facts when it has a row
pub fn widen_row<Backend: Io>(
    state: &EngineState<Backend>,
    index: usize,
    rows: &mut SummaryRows,
    number: SegmentNumber,
    counter: WritePlace,
) {
    let ranges = whole_columns(state, index, number, counter);
    let mut facts = SegmentFacts {
        segment: number,
        ..SegmentFacts::default()
    };
    let mut columns = Vec::new();
    if let Some(row) = rows.get(number) {
        facts = row.facts;
        let _spans = row_ranges(&row, number, &mut columns);
    }
    facts.is_damaged = true;
    facts.damage_place = counter;
    rows.remove(number);
    state.note_dropped_rows(rows.push_kept(&facts, columns.iter().map(column_of), ranges.iter().map(span_of)));
}

/// Damage whole what a row the newer summary copy lost could have spoken for: an older row may lack the
/// newer row's spans, and a segment with no row may have had some. Older rows keep their facts
pub fn mark_lost_rows<Backend: Io>(
    state: &EngineState<Backend>,
    scan: &RootScan<Backend::File>,
    index: usize,
    rows: &mut SummaryRows,
) {
    let lost = mem::take(&mut *hold(&state.volumes[index].lost_rows));
    let counter = state.order.mark();
    for number in &lost.unread_covers {
        widen_row(state, index, rows, *number, counter);
    }
    // a row from the older copy can sit behind counts the newer rows already hold, so only a backward walk is safe
    for number in &lost.older {
        rows.restart_backward(*number);
    }
    if !lost.has_lost_row {
        return;
    }

    for number in lost.older {
        widen_row(state, index, rows, number, counter);
    }

    let sealed = scan.sealed.iter().filter(|found| found.volume == index).map(|found| found.head.segment);
    let open = scan.scanned.iter().filter(|segment| segment.found.volume == index).map(|segment| segment.found.number);
    for number in sealed.chain(open) {
        if rows.get(number).is_some() {
            continue;
        }
        let facts = SegmentFacts {
            segment: number,
            is_damaged: true,
            damage_place: counter,
            ..SegmentFacts::default()
        };
        let ranges = whole_columns(state, index, number, counter);
        state.note_dropped_rows(rows.push_kept(&facts, [].into_iter(), ranges.iter().map(span_of)));
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
                path: found.root.clone(),
                number: found.number,
                file_length: found.file_length,
                head: None,
                columns: Vec::new(),
            });
            continue;
        }
        let _is_gone = state.remove_or_park(&state.volumes[volume], &found.root, found.number);
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
        marks: found.head.marks,
        is_damaged: false,
        has_covers: found.head.ranges_length.0 > 0,
        damage_place: WritePlace(0),
        block_count: found.head.block_count.0,
        walked_blocks: 0,
        // no saved count can hold a walk of a segment above the number, one at or below it may be held already, and an output
        // holds rows its input's walk already counted
        is_backward: found.head.segment <= rows.highest_forward || (found.head.is_output && !found.head.is_unwalked),
        has_lost_rows: false,
    };

    if let Some(at) = listed.get(&facts.segment)
        && let Some(row) = rows.row_at(*at)
        && is_same_segment(&row.facts, &facts)
    {
        // spans outlive the fault that made them, and only clear_damage ends them
        if row.facts.is_damaged {
            for range in row_ranges(&row, facts.segment, &mut Vec::new()) {
                volume.damage.add(&range);
            }
            hold(&volume.damaged).push(facts.segment);
        }
        // the room was sized at open from every row it found, and a row with spans has to stay
        settled.make_room_for(rows, *at);
        let is_copied = settled.copy_row(rows, *at);
        debug_assert!(is_copied);
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
        dead_rows: RowCount(0),
        dead_graves: RowCount(0),
        live_bytes: ByteCount(0),
        dead_bytes: ByteCount(0),
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
        facts.has_lost_rows = row.facts.has_lost_rows;
        for range in &ranges {
            volume.damage.add(range);
        }
        hold(&volume.damaged).push(facts.segment);
    }
    if ranges.is_empty() {
        if !settled.push(&facts, columns, [].into_iter()) {
            state.note_dropped_row();
        }
    } else {
        state.note_dropped_rows(settled.push_kept(&facts, columns, ranges.iter().map(span_of)));
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

    // a row can give the keys a segment held, and never the ranges its lost footer hid, so those go whole
    match row.filter(|row| is_row_trusted(row, found) && !row.facts.has_covers) {
        Some(row) => {
            facts = row.facts;
            facts.is_damaged = true;
            ranges = row_ranges(&row, found.number, &mut columns);
            // damage from before the tails went ranks below any row that landed after it, and the lost footer hid those too.
            // Once ranked at the highest place this stops, so a span a caller cleared stays cleared
            if row.facts.damage_place < row.facts.places.highest {
                facts.damage_place = row.facts.places.highest;
                if row.facts.is_damaged {
                    ranges.extend(column_ranges(&columns, found.number, row.facts.places.highest));
                }
            }
        },
        None => fallback_ranges(state, found, &mut facts, &mut ranges),
    }

    for range in &ranges {
        volume.damage.add(range);
    }
    hold(&volume.damaged).push(found.number);
    let column_facts = columns.iter().map(column_of);
    state.note_dropped_rows(settled.push_kept(&facts, column_facts, ranges.iter().map(span_of)));
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
            dead_rows: column.dead_rows,
            dead_graves: column.dead_graves,
            live_bytes: column.live_bytes,
            dead_bytes: column.dead_bytes,
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

    ranges.extend(column_ranges(columns, segment, highest_place));
    ranges
}

/// Cover each of a row's columns from its lowest key to its bound
pub fn column_ranges<'columns>(
    columns: &'columns [OwnedColumn],
    segment: SegmentNumber,
    highest_place: WritePlace,
) -> impl Iterator<Item = DamagedRange> + 'columns {
    columns.iter().map(move |column| DamagedRange {
        column: column.column,
        segment,
        highest_place,
        start: column.lowest.clone(),
        end: (!column.bound.is_empty()).then(|| column.bound.clone()),
    })
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

/// Check a row and a tail describe the same segment, whatever its row says about damage or the walk
pub fn is_same_segment(row: &SegmentFacts, tail: &SegmentFacts) -> bool {
    let without_damage = SegmentFacts {
        is_damaged: false,
        damage_place: WritePlace(0),
        // a settled segment's row drops the flag its tail keeps for good
        has_covers: tail.has_covers,
        walked_blocks: tail.walked_blocks,
        is_backward: tail.is_backward,
        has_lost_rows: false,
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

/// Read an owned column as the facts a row keeps
pub fn column_of(column: &OwnedColumn) -> ColumnFacts<'_> {
    ColumnFacts {
        column: column.column,
        lowest: &column.lowest,
        bound: &column.bound,
        row_count: column.row_count,
        grave_count: column.grave_count,
        dead_rows: column.dead_rows,
        dead_graves: column.dead_graves,
        live_bytes: column.live_bytes,
        dead_bytes: column.dead_bytes,
    }
}

/// Write what's left of every damaged segment's spans into its row, in place, which never fails: a row with
/// spans stays, and a segment with no row gets one that speaks for its spans alone
pub fn save_cleared_spans<Backend: Io>(state: &EngineState<Backend>, volume: &Volume<Backend>) {
    let mut rows = hold(&volume.summary);
    let damaged = hold(&volume.damaged);
    let slots = volume.damage.slots();
    for number in damaged.iter() {
        let spans = spans_of(&slots, *number);
        match rows.rewrite_spans(*number, spans.clone()) {
            Some(dropped) => state.note_dropped_rows(dropped),
            None => {
                let facts = SegmentFacts {
                    segment: *number,
                    is_damaged: true,
                    damage_place: highest_of(&slots, *number).unwrap_or(WritePlace(0)),
                    ..SegmentFacts::default()
                };
                state.note_dropped_rows(rows.push_kept(&facts, [].into_iter(), spans));
            },
        }
    }
}

impl<Backend: Io> EngineState<Backend> {
    /// Damage a segment's columns whole at the counter, when a seal finds a cover it can't read. The row it leaves is an
    /// open segment's, which open carries over to the sealed segment, and the seal saves it before its tail is durable
    pub fn damage_whole(&self, volume: usize, segment: SegmentNumber) {
        let counter = self.order.mark();
        let ranges = whole_columns(self, volume, segment, counter);
        let target = &self.volumes[volume];
        for range in &ranges {
            target.damage.add(range);
        }
        let mut damaged = hold(&target.damaged);
        if !damaged.contains(&segment) {
            damaged.push(segment);
            self.damaged_segments.fetch_add(1, Ordering::AcqRel);
        }
        drop(damaged);

        let facts = SegmentFacts {
            segment,
            is_damaged: true,
            damage_place: counter,
            ..SegmentFacts::default()
        };
        let mut rows = hold(&target.summary);
        rows.remove(segment);
        let dropped = rows.push_kept(&facts, [].into_iter(), ranges.iter().map(span_of));
        drop(rows);
        self.note_dropped_rows(dropped);
    }
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

/// Open a file the way this open needs it
pub fn open_for<Backend: Io>(io: &Backend, path: &Path, mode: OpenMode) -> Result<Backend::File> {
    match mode {
        OpenMode::Writable => io.open_file(path),
        OpenMode::ReadOnly => io.open_read_only(path),
    }
}

/// Find which of a volume's roots a file sits under
pub fn root_of_path<Backend: Io>(volume: &Volume<Backend>, path: &Path) -> RootId {
    RootId(volume.roots.iter().position(|root| path.starts_with(root)).unwrap_or(0) as u8)
}
