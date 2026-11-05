//! Open's reading of a root: sealed segments from their tails, open ones from their rows

use std::io::{Error, ErrorKind, Result};
use std::path::Path;
use std::sync::atomic::Ordering;

use reel_api::units::{ByteCount, FileOffset, RootId, RowCount, WritePlace};

use crate::engine::engine_sizing::GROUP_BYTES;
use crate::engine::engine_state::EngineState;
use crate::engine::open_segment::{DATA_SUFFIX, OpenSegment, SegmentPhase, data_path, parse_segment, rows_path};
use crate::engine::volume::Volume;
use crate::guard::hold;
use crate::index::open_slots::OpenSlot;
use crate::index::write_row::WriteRow;
use crate::io::{Io, IoFile};
use crate::record::record_check::RecordIdentity;
use crate::record::record_reader::{RecordLocation, RecordRead, RecordReader};
use crate::row_kind::RowKind;
use crate::rows::row_group::{GROUP_HEADER_BYTES, RowEntry, TOTAL_LENGTH_AT, parse_group, read_half};
use crate::segment::column_directory::DirectoryEntry;
use crate::segment::footer_reader::FooterReader;
use crate::segment::segment_tail::{ColumnTail, SegmentNumber, TailHead};

/// What a read of one root found
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RootFindings {
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

impl<Backend: Io> EngineState<Backend> {
    /// Read every segment of a root, sealed and open
    pub fn recover_root(&self, volume: &Volume<Backend>, root: RootId) -> Result<RootFindings> {
        let path = &volume.roots[root.0 as usize];
        let mut findings = RootFindings::default();
        let mut reader = FooterReader::new();
        let mut records = RecordReader::new();

        // every root is listed before any tail is read, and each file is read once
        let listing = self.io.list_directory(path)?;
        for file_path in &listing {
            let Some(number) = parse_segment(file_path, DATA_SUFFIX) else {
                continue;
            };
            findings.highest_segment = findings.highest_segment.max(number);
            let file = self.io.open_file(file_path)?;

            let mut columns = Vec::new();
            match reader.read_tail(&file, self.record_key, &mut columns)? {
                Some(head) => {
                    self.install_sealed(volume, path, file, &head, &columns, &mut reader)?;
                    findings.highest_place = findings.highest_place.max(head.places.highest);
                },
                None => {
                    let place = self.recover_open(volume, root, number, file, &mut records)?;
                    findings.highest_place = findings.highest_place.max(place);
                },
            }
        }
        Ok(findings)
    }

    /// Put a sealed segment in the table and the directories
    pub fn install_sealed(
        &self,
        volume: &Volume<Backend>,
        path: &Path,
        file: Backend::File,
        head: &TailHead,
        columns: &[ColumnTail],
        reader: &mut FooterReader,
    ) -> Result<()> {
        let mut entries = Vec::new();
        if !reader.read_index(&file, head, &mut entries)? {
            return Err(Error::from(ErrorKind::InvalidData));
        }

        // a kill between a footer's sync and its rows file going leaves the rows behind
        match self.io.remove_file(&rows_path(path, head.segment)) {
            Ok(()) => {},
            Err(error) if error.kind() == ErrorKind::NotFound => {},
            Err(error) => return Err(error),
        }

        if !volume.table.install(file, head, columns, &entries) {
            return Err(Error::other("segment table is full"));
        }
        for column in columns {
            let entry = DirectoryEntry {
                shared_length: column.shared_length,
                shared_sum: column.shared_sum,
                lowest: column.lowest,
                highest: column.highest,
                reach: column.highest,
                segment: head.segment,
            };
            if let Some(directory) = &self.directories[column.column.0 as usize]
                && !directory.replace(Some(&entry), &[])
            {
                return Err(Error::other("column directory is full"));
            }
        }
        Ok(())
    }

    /// Rebuild an open segment from its rows, and mark it to seal
    pub fn recover_open(
        &self,
        volume: &Volume<Backend>,
        root: RootId,
        number: SegmentNumber,
        data: Backend::File,
        records: &mut RecordReader,
    ) -> Result<WritePlace> {
        let path = &volume.roots[root.0 as usize];
        let rows = match self.io.open_file(&rows_path(path, number)) {
            Ok(rows) => rows,
            Err(error) if error.kind() == ErrorKind::NotFound => {
                self.io.close_file(data);
                self.io.remove_file(&data_path(path, number))?;
                return Ok(WritePlace(0));
            },
            Err(error) => return Err(error),
        };

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
        let kept_end = self.replay_rows(&rows, &data, slot, &scan, records, &mut recovered)?;
        rows.truncate(ByteCount(kept_end.0))?;

        hold(&self.queues[slot.0 as usize]).reset(kept_end, scan.synced_through);
        self.slot_files[slot.0 as usize].set(Some(data.clone()));
        volume.open_slot(slot);
        hold(&volume.claims).open.push(OpenSegment {
            number,
            slot,
            root,
            data,
            rows,
            phase: SegmentPhase::Sealing,
            write_end: recovered.write_end,
            row_count: recovered.row_count,
            in_flight: 0,
            lowest_place: recovered.lowest_place,
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
        data: &Backend::File,
        slot: OpenSlot,
        scan: &RowsScan,
        records: &mut RecordReader,
        recovered: &mut RecoveredRows,
    ) -> Result<FileOffset> {
        let mut bytes = vec![0; GROUP_BYTES.0 as usize];
        let mut value = Vec::new();
        let mut at = FileOffset(0);

        while at.0 < scan.valid_end.0 {
            let Some(length) = read_group(rows, at, scan.valid_end_length(), &mut bytes)? else {
                break;
            };
            let Some(group) = parse_group(&bytes[..length], self.record_key) else {
                break;
            };

            // a group past the last sync may list a record a power cut lost, and it goes whole
            if at.0 >= scan.synced_through.0 {
                for row in group.walk() {
                    if !self.is_record_whole(data, &row, records, &mut value)? {
                        return Ok(at);
                    }
                }
            }

            for row in group.walk() {
                self.replay_row(&row, slot, recovered);
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
    pub fn replay_row(&self, row: &RowEntry, slot: OpenSlot, recovered: &mut RecoveredRows) {
        let write_row = WriteRow {
            place: row.place,
            offset: row.offset,
            length: row.length,
            kind: row.kind,
            slot,
        };
        while !self.index.insert(row.column, row.key, &write_row) {
            self.io.pause();
        }

        recovered.row_count = RowCount(recovered.row_count.0 + 1);
        recovered.lowest_place = recovered.lowest_place.min(row.place);
        recovered.highest_place = recovered.highest_place.max(row.place);
        recovered.write_end = recovered.write_end.max(FileOffset(row.offset.0 + row.length.0));
    }

    /// Note a column's rows moved into the order's history
    pub fn resume_order(&self, findings: &RootFindings) {
        self.order.resume_after(findings.highest_place);
        let next = findings.highest_segment.0 + 1;
        self.next_segment.fetch_max(next, Ordering::AcqRel);
    }

}

impl RowsScan {
    /// Read how far a replay may read
    pub fn valid_end_length(&self) -> ByteCount {
        ByteCount(self.valid_end.0)
    }
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
