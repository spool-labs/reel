//! Sweep a volume's records against their checksums
//!
//! A sealed segment is swept through its footer, which names where each record
//! it indexes sits; a segment with no footer is walked record by record from the
//! start until the write frontier. Nothing here writes, and nothing is repaired.
//! A sweep only means what it says on a volume nothing is appending to.

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use crate::engine::ReelStore;
use crate::format::column::MAX_KEY_LEN;
use crate::format::footer::{SegmentFooter, NO_RECORD};
use crate::format::loc::SegmentId;
use crate::format::record::{RecordHeader, HEADER_LEN};
use crate::reel::{segment_file_name, SEGMENT_SUFFIX};

/// One segment's sweep, and the first thing wrong with it
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct VerifyRow {
    /// Segment number, taken from the file's name
    pub segment: u32,

    /// Whether it was swept through a footer rather than walked
    pub sealed: bool,

    /// Whether the index names the file at all
    pub indexed: bool,

    /// Records that read and matched their checksum
    pub records: u64,

    /// Bytes those records span on disk
    pub bytes: u64,

    /// Rows whose value lives in the footer alone, with no record to read
    pub carried: u64,

    /// Records that would not read or would not match
    pub faults: u64,

    /// The first fault, which is the one the report names
    pub fault: Option<String>,
}

impl VerifyRow {
    fn new(segment: u32) -> VerifyRow {
        VerifyRow {
            segment,
            sealed: false,
            indexed: false,
            records: 0,
            bytes: 0,
            carried: 0,
            faults: 0,
            fault: None,
        }
    }

    /// One sound record of this many bytes
    fn sound(&mut self, bytes: u64) {
        self.records += 1;
        self.bytes += bytes;
    }

    /// One fault, keeping the first as the one the report names
    fn fault(&mut self, why: String) {
        self.faults += 1;
        self.fault.get_or_insert(why);
    }

    /// A segment that could not be swept at all
    fn faulted(mut self, why: String) -> VerifyRow {
        self.fault(why);
        self
    }
}

/// What a sweep found, and what it did not look at
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct VerifyReport {
    /// The volume's root directory
    pub volume: String,

    /// Segment files swept, before the listing below is truncated
    pub segments_swept: usize,

    /// Records checked across every segment
    pub records: u64,

    /// Bytes those records span on disk
    pub bytes: u64,

    /// Rows carried in a footer, with no record to read
    pub carried_rows: u64,

    /// Records that would not read or would not match
    pub faults: u64,

    /// Reads the engine itself failed, counted before this sweep read a byte
    pub unreadable_records: u64,

    /// Segment files the index does not name, so nothing reads them
    pub not_indexed: Vec<String>,

    /// Segments, the faulted ones first, truncated to the limit asked for
    pub segments: Vec<VerifyRow>,
}

impl VerifyReport {
    /// Whether the sweep found nothing wrong anywhere
    pub fn is_sound(&self) -> bool {
        self.faults == 0 && self.unreadable_records == 0 && self.not_indexed.is_empty()
    }
}

/// Sweep every record an open volume holds against its checksum
///
/// Driven by what is on the disk rather than by what the index remembers: a file
/// the index never named is exactly the file a sweep must not skip. Open the
/// volume paged, since a resident open names only the segments still holding a
/// live key.
pub fn verify(engine: &ReelStore, limit: usize) -> VerifyReport {
    let files = segment_files(&roots(engine));
    let named: Vec<SegmentId> = engine
        .index()
        .segments_snapshot()
        .into_iter()
        .map(|(segment, _)| segment)
        .collect();
    let mut rows: Vec<VerifyRow> = files
        .iter()
        .map(|(segment, path)| sweep(engine, *segment, path, named.contains(segment)))
        .collect();
    rows.sort_by_key(|row| row.segment);
    VerifyReport {
        volume: engine.root().display().to_string(),
        segments_swept: rows.len(),
        records: rows.iter().map(|row| row.records).sum(),
        bytes: rows.iter().map(|row| row.bytes).sum(),
        carried_rows: rows.iter().map(|row| row.carried).sum(),
        faults: rows.iter().map(|row| row.faults).sum(),
        unreadable_records: engine.unreadable_records(),
        not_indexed: rows
            .iter()
            .filter(|row| !row.indexed)
            .map(|row| segment_file_name(SegmentId(row.segment)))
            .collect(),
        segments: {
            // The faulted ones first, since a sweep is run to find them.
            rows.sort_by(|a, b| b.faults.cmp(&a.faults).then(a.segment.cmp(&b.segment)));
            rows.truncate(limit);
            rows
        },
    }
}

/// Every segment file on the volume's roots, in segment order
fn segment_files(roots: &[PathBuf]) -> Vec<(SegmentId, PathBuf)> {
    let mut files = Vec::new();
    for root in roots {
        let Ok(entries) = std::fs::read_dir(root) else {
            continue;
        };
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().to_string();
            let Some(number) = name.strip_suffix(SEGMENT_SUFFIX) else {
                continue;
            };
            if let Ok(number) = number.parse::<u32>() {
                files.push((SegmentId(number), entry.path()));
            }
        }
    }
    files.sort();
    files
}

/// The roots a segment of this volume can sit under, in the volume's own order
fn roots(engine: &ReelStore) -> Vec<PathBuf> {
    std::iter::once(engine.root().to_path_buf())
        .chain(
            engine
                .config()
                .volumes
                .iter()
                .map(|volume| volume.path.clone()),
        )
        .collect()
}

/// Sweep one segment file, through its footer where it has one
fn sweep(engine: &ReelStore, segment: SegmentId, path: &Path, indexed: bool) -> VerifyRow {
    let mut row = VerifyRow::new(segment.as_u32());
    row.indexed = indexed;
    let name = segment_file_name(segment);
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(error) => return row.faulted(format!("{name} will not open: {error}")),
    };
    let len = match file.metadata() {
        Ok(data) => data.len(),
        Err(error) => return row.faulted(format!("{name} will not stat: {error}")),
    };
    // A footer says where every record it indexes sits, so a sealed segment is
    // swept through it. Without one there is no boundary between the records and
    // whatever follows them, so the segment is walked instead.
    match engine.segment_footer(segment) {
        Ok(Some(footer)) => {
            row.sealed = true;
            sweep_footer(&mut file, &footer, &mut row);
        }
        Ok(None) => walk(&mut file, len, &mut row),
        Err(error) => row.fault(format!("footer does not parse: {error}")),
    }
    row
}

/// Check every record a footer indexes, in the order they sit on disk
fn sweep_footer(file: &mut File, footer: &SegmentFooter, row: &mut VerifyRow) {
    let mut at: Vec<(u32, u16, u32)> = Vec::new();
    for entry in footer.entries() {
        match entry {
            // A row whose value lives in the footer alone has no record to read.
            Ok(entry) if entry.offset == NO_RECORD => row.carried += 1,
            Ok(entry) => at.push((entry.offset, entry.key.width(), entry.len)),
            Err(error) => row.fault(format!("footer row does not decode: {error}")),
        }
    }
    // Ascending, so a sweep of a spinning disk reads the file forwards.
    at.sort_unstable();
    for (offset, width, len) in at {
        let span = HEADER_LEN as u64 + u64::from(width) + u64::from(len);
        match check(file, u64::from(offset), span) {
            Checked::Sound(bytes) => row.sound(bytes),
            Checked::Fault(why) => row.fault(why),
            // A footer named the record, so unwritten space where it pointed is
            // the pointer being wrong rather than the end of anything.
            Checked::Frontier => row.fault(format!("record at {offset} is unwritten space")),
        }
    }
}

/// Walk a segment with no footer, record by record, up to its write frontier
fn walk(file: &mut File, len: u64, row: &mut VerifyRow) {
    let mut at = 0u64;
    while at + HEADER_LEN as u64 <= len {
        match check(file, at, (HEADER_LEN + MAX_KEY_LEN) as u64) {
            Checked::Sound(bytes) => {
                row.sound(bytes);
                at += bytes;
            }
            Checked::Fault(why) => {
                row.fault(why);
                return;
            }
            Checked::Frontier => return,
        }
    }
}

/// What one record's bytes came back as
enum Checked {
    /// The record checks out, and this is what it spans on disk
    Sound(u64),

    /// The record is not sound, and this says why
    Fault(String),

    /// Unwritten space, so a walk has reached the frontier and stops
    Frontier,
}

/// Read the record at this offset and check it against its own checksum
///
/// The hint is what to read before the header has said how long the record is:
/// a footer knows exactly, and a walk asks for a header and the widest key the
/// format admits.
fn check(file: &mut File, at: u64, hint: u64) -> Checked {
    let head = match read_at(file, at, hint) {
        Ok(head) => head,
        Err(error) => return Checked::Fault(format!("read at {at} failed: {error}")),
    };
    let header = match RecordHeader::unpack(&head) {
        Ok(header) => header,
        Err(error) => return Checked::Fault(format!("header at {at} does not parse: {error}")),
    };
    if header.is_unwritten() {
        return Checked::Frontier;
    }
    // A pad's fill is never written and never checksummed, so it is stepped over
    // rather than read.
    if header.flags.is_pad() {
        return Checked::Sound(header.span());
    }
    let payload = match header.has_payload() {
        false => Vec::new(),
        true => match read_at(file, at + header.prefix_len(), u64::from(header.length)) {
            Ok(payload) => payload,
            Err(error) => return Checked::Fault(format!("payload at {at} is short: {error}")),
        },
    };
    match header.verify(&payload) {
        true => Checked::Sound(header.span()),
        false => Checked::Fault(format!("record at {at} fails its checksum")),
    }
}

/// Read up to this many bytes from an offset, short at the end of the file
fn read_at(file: &mut File, at: u64, len: u64) -> std::io::Result<Vec<u8>> {
    file.seek(SeekFrom::Start(at))?;
    let mut bytes = Vec::new();
    file.take(len).read_to_end(&mut bytes)?;
    Ok(bytes)
}
