//! Open's reading of the roots: sealed segments from their tails, open ones from their rows

use std::io::{Error, ErrorKind, Result};
use std::path::PathBuf;
use std::sync::atomic::Ordering;

use reel_api::units::{ByteCount, FileOffset, RootId, RowCount, WritePlace};

use crate::engine::engine_sizing::GROUP_BYTES;
use crate::engine::engine_state::{EngineState, SlotData};
use crate::engine::open_segment::{DATA_SUFFIX, OpenSegment, SegmentPhase, data_path, parse_segment, rows_path};
use crate::engine::volume::Volume;
use crate::guard::hold;
use crate::index::open_slots::OpenSlot;
use crate::index::write_row::WriteRow;
use crate::io::{Io, IoFile};
use crate::record::record_check::{RecordIdentity, RecordKey};
use crate::record::record_head::SMALL_HEAD_BYTES;
use crate::record::record_reader::{RecordLocation, RecordRead, RecordReader};
use crate::row_kind::RowKind;
use crate::rows::row_group::{GROUP_HEADER_BYTES, RowEntry, TOTAL_LENGTH_AT, parse_group, read_half};
use crate::segment::block_entry::BlockEntry;
use crate::segment::column_directory::DirectoryEntry;
use crate::segment::footer_reader::FooterReader;
use crate::segment::segment_table::{TABLE_SHARDS, TableSizing, shard_of};
use crate::segment::segment_tail::{ColumnTail, SegmentNumber, TailHead};

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
    pub damaged: u64,
    pub highest_place: WritePlace,
    pub highest_segment: SegmentNumber,
}

/// What a walk of one rows file found
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RowsScan {
    pub valid_end: FileOffset,
    pub synced_through: FileOffset,
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
        damaged: 0,
        highest_place: WritePlace(0),
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
            // the file stays for an operator, and the rest of the store keeps serving
            scan.damaged += 1;
            io.close_file(file);
            return Ok(());
        }
        scan.highest_place = scan.highest_place.max(head.places.highest);
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
    io.close_file(file);
    if is_empty {
        // a crash between creating the two files of a segment leaves this
        io.remove_file(&data_path(&listing.path, number))?;
    } else {
        scan.damaged += 1;
    }
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

    /// Rebuild an open segment from its rows, ready to go on taking writes
    pub fn recover_open(&self, volume: &Volume<Backend>, found: FoundOpen<Backend::File>) -> Result<WritePlace> {
        let path = &volume.roots[found.root.0 as usize];
        let rows = self.io.open_file(&rows_path(path, found.number))?;
        let Some(slot) = self.take_slot() else {
            return Err(Error::other("more open segments than slots"));
        };

        let scan = self.scan_rows(&rows)?;
        let mut recovered = RecoveredRows {
            write_end: FileOffset(0),
            row_count: RowCount(0),
            lowest_place: WritePlace(u64::MAX),
            highest_place: WritePlace(0),
        };
        let mut records = RecordReader::new();
        let target = ReplayTarget {
            data: &found.data,
            slot,
            segment: found.number,
        };
        let kept_end = self.replay_rows(&rows, &target, &scan, &mut records, &mut recovered)?;
        rows.truncate(ByteCount(kept_end.0))?;

        let synced_through = FileOffset(scan.synced_through.0.min(kept_end.0));
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
            data: found.data,
            rows,
            phase: SegmentPhase::Taking,
            write_end: recovered.write_end,
            row_count: recovered.row_count,
            in_flight: 0,
            lowest_place: recovered.lowest_place,
            highest_place: recovered.highest_place,
        });
        Ok(recovered.highest_place)
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
            scan.valid_end = FileOffset(scan.valid_end.0 + group_length as u64);
        }
        Ok(scan)
    }

    /// Put each group's rows in the index, checking the records of groups no sync covered
    pub fn replay_rows(
        &self,
        rows: &Backend::File,
        target: &ReplayTarget<Backend::File>,
        scan: &RowsScan,
        records: &mut RecordReader,
        recovered: &mut RecoveredRows,
    ) -> Result<FileOffset> {
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
                        return Ok(at);
                    }
                }
            }

            for row in group.walk() {
                self.replay_row(&row, target, recovered)?;
            }
            at = FileOffset(at.0 + length as u64);
        }
        Ok(at)
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

    /// Put one recovered row in the write index
    pub fn replay_row(
        &self,
        row: &RowEntry,
        target: &ReplayTarget<Backend::File>,
        recovered: &mut RecoveredRows,
    ) -> Result<()> {
        let write_row = WriteRow {
            place: row.place,
            offset: row.offset,
            length: row.length,
            kind: row.kind,
            slot: target.slot,
            segment: target.segment,
        };
        if !self.index.insert(row.column, row.key, &write_row) {
            return Err(Error::other("open segments hold more rows than this budget's write index"));
        }

        recovered.row_count = RowCount(recovered.row_count.0 + 1);
        recovered.lowest_place = recovered.lowest_place.min(row.place);
        recovered.highest_place = recovered.highest_place.max(row.place);
        recovered.write_end = recovered.write_end.max(FileOffset(row.offset.0 + row.length.0));
        Ok(())
    }

    /// Carry on the write order and the segment numbers from what open found
    pub fn resume_order(&self, highest_place: WritePlace, highest_segment: SegmentNumber) {
        self.order.resume_after(highest_place);
        self.next_segment.fetch_max(highest_segment.0 + 1, Ordering::AcqRel);
    }
}

/// Where recovered rows go
#[derive(Clone, Copy, Debug)]
pub struct ReplayTarget<'file, File> {
    pub data: &'file File,
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
