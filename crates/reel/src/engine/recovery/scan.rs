//! Listing a volume's roots at open and reading what each segment file holds

use std::io::{Error, ErrorKind, Result};
use std::path::{Path, PathBuf};

use reel_api::units::{FileOffset, RootId};

use crate::engine::open_segment::{DATA_SUFFIX, OUTPUT_SUFFIX, data_path, parse_segment, rows_path};
use crate::engine::volume::Volume;
use crate::io::device_error::is_device_error;
use crate::io::{Io, IoFile};
use crate::record::record_check::RecordKey;
use crate::record::record_head::SMALL_HEAD_BYTES;
use crate::segment::footer_reader::FooterReader;
use crate::segment::segment_table::{TABLE_SHARDS, TableSizing, shard_of};
use crate::segment::segment_tail::{SegmentNumber, columns_fit};
use crate::segment::summary::{
    MOST_ROW_COLUMN_BYTES, MOST_ROW_FIXED_BYTES,
};

use crate::engine::recovery::{FoundDamaged, FoundEmpty, FoundOpen, FoundSealed, HeldFile, OpenMode, RootListing, RootScan, ScannedOpen, SegmentAt, SegmentRead, is_tail_stale, walk_listed_rows};

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
    let inputs: Vec<SegmentNumber> = scan.sealed.iter().filter(is_final).flat_map(|found| found.inputs.inputs()).copied().collect();
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
        let inputs = scan.sealed[index].inputs.inputs();
        let is_held = |number: &SegmentNumber| scan.sealed.iter().any(|found| found.head.segment == *number);
        if !head.is_partial || !inputs.iter().any(is_held) {
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
        SegmentRead::Sealed { head, inputs, columns, entries } => scan.sealed.push(FoundSealed {
            volume: listing.volume,
            path: listing.path.clone(),
            file,
            head,
            inputs,
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
            return Ok(SegmentRead::Sealed { head, inputs: reader.tail_inputs, columns, entries });
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
