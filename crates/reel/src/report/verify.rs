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
use crate::report::caveat::{self, Caveat};
use crate::report::doc::{Column, Doc, Note, Row, Table, Tone};
use crate::report::fmt;
use crate::report::render::Report;

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

    /// Whether the segment carries nothing a listing would be read for
    ///
    /// A volume seals segments whose every record has since been superseded, and
    /// they sweep clean with nothing in them. They are swept and counted either
    /// way; this is only whether a row of zeroes is worth a reader's line.
    pub fn is_empty(&self) -> bool {
        self.records == 0 && self.faults == 0 && self.carried == 0
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

    /// Segment files holding records the index does not name, so nothing reads them
    ///
    /// A segment holding nothing but its own header is left out: an idle tail
    /// keeps one across a close for the next open to resume, and the index names
    /// a segment only once a record has landed in it.
    pub not_indexed: Vec<String>,

    /// Segments swept that hold no record at all, which is a listing's noise
    pub empty_segments: usize,

    /// Segments swept that carry something, before the listing is truncated
    pub holding_segments: usize,

    /// Segments that faulted, before the listing is truncated
    ///
    /// The verdict counts from here rather than from the rows below it: a
    /// listing capped at twenty rows would otherwise report twenty faulted
    /// segments however many faulted.
    pub faulted_segments: usize,

    /// Segments, the faulted ones first, truncated to the limit asked for
    pub segments: Vec<VerifyRow>,

    /// What stands between these figures and what a reader would take them for
    pub caveats: Vec<Caveat>,
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
    verify_watched(engine, limit, &mut |_| {})
}

/// Sweep, telling a watcher how far it has got as it goes
///
/// A sweep of a full volume is minutes of reading with nothing to show for it
/// until the end, so a frontend that has somebody waiting takes this form and
/// draws what comes back. The watcher is called often and told everything; how
/// often that is worth drawing is the frontend's to decide, not the engine's.
pub fn verify_watched(
    engine: &ReelStore,
    limit: usize,
    tell: &mut dyn FnMut(&Swept),
) -> VerifyReport {
    let files = segment_files(&roots(engine));
    let named: Vec<SegmentId> = engine
        .index()
        .segments_snapshot()
        .into_iter()
        .map(|(segment, _)| segment)
        .collect();

    let mut watch = Watch {
        swept: Swept {
            segments_done: 0,
            segments_total: files.len(),
            bytes_done: 0,
            bytes_total: files.iter().map(|file| file.weight).sum(),
        },
        finished: 0,
        since_tell: 0,
        tell,
    };
    watch.say();

    let mut rows: Vec<VerifyRow> = Vec::with_capacity(files.len());
    for file in &files {
        let indexed = named.contains(&file.segment);
        rows.push(sweep(engine, file, indexed, &mut watch));
        watch.segment_done(file.weight);
    }
    rows.sort_by_key(|row| row.segment);

    let empty_segments = rows.iter().filter(|row| row.is_empty()).count();
    // A file holding nothing is nothing to lose: an idle tail keeps its
    // header-only segment across a close so the next open resumes it, and no
    // index names a segment before a record lands in it.
    let not_indexed: Vec<String> = rows
        .iter()
        .filter(|row| !row.indexed && !row.is_empty())
        .map(|row| segment_file_name(SegmentId(row.segment)))
        .collect();

    VerifyReport {
        volume: engine.root().display().to_string(),
        segments_swept: rows.len(),
        records: rows.iter().map(|row| row.records).sum(),
        bytes: rows.iter().map(|row| row.bytes).sum(),
        carried_rows: rows.iter().map(|row| row.carried).sum(),
        faults: rows.iter().map(|row| row.faults).sum(),
        unreadable_records: engine.unreadable_records(),
        caveats: caveats(&not_indexed, engine.unreadable_records()),
        not_indexed,
        empty_segments,
        holding_segments: rows.len() - empty_segments,
        faulted_segments: rows.iter().filter(|row| row.faults > 0).count(),
        segments: {
            // The faulted ones first, since a sweep is run to find them, and the
            // segments carrying nothing last, so a limit spends its rows on the
            // segments that have something to say.
            rows.sort_by(|a, b| {
                b.faults
                    .cmp(&a.faults)
                    .then(a.is_empty().cmp(&b.is_empty()))
                    .then(a.segment.cmp(&b.segment))
            });
            // A limit of zero asks for the whole listing rather than none of it.
            if limit > 0 {
                rows.truncate(limit);
            }
            rows
        },
    }
}

/// What a reader has to know beyond the counts, where anything is unaccounted
fn caveats(not_indexed: &[String], unreadable: u64) -> Vec<Caveat> {
    let mut caveats = Vec::new();
    if !not_indexed.is_empty() {
        caveats.push(
            Caveat::new(format!(
                "{} segment files the index does not name, so nothing reads them: {}",
                not_indexed.len(),
                listed(not_indexed),
            ))
            .fix("a volume whose files are all here has an unreadable format or a lost index"),
        );
    }
    if unreadable > 0 {
        caveats.push(Caveat::new(format!(
            "the engine itself failed {unreadable} reads before this sweep read a byte",
        )));
    }
    caveats
}

/// The first few of a list, with the rest counted rather than printed
fn listed(names: &[String]) -> String {
    const SHOWN: usize = 8;
    match names.len() > SHOWN {
        true => format!(
            "{}, and {} more",
            names[..SHOWN].join(", "),
            names.len() - SHOWN
        ),
        false => names.join(", "),
    }
}

/// How far a sweep has got, for a frontend with somebody waiting on it
#[derive(Clone, Copy, Debug)]
pub struct Swept {
    /// Segment files finished
    pub segments_done: usize,

    /// Segment files there are to finish
    pub segments_total: usize,

    /// Bytes read and checked so far
    pub bytes_done: u64,

    /// Bytes the files weigh, which is what the sweep is working through
    pub bytes_total: u64,
}

impl Swept {
    /// How far through, as the fraction a bar is drawn from
    ///
    /// Falls back to counting files where the files weigh nothing the sweep can
    /// divide by, which is a volume of empty segments and a volume of none.
    pub fn fraction(&self) -> f64 {
        match self.bytes_total {
            0 => match self.segments_total {
                0 => 1.0,
                total => self.segments_done as f64 / total as f64,
            },
            total => (self.bytes_done as f64 / total as f64).clamp(0.0, 1.0),
        }
    }
}

/// The sweep's progress and whoever asked to hear about it
///
/// Progress is counted as files finished plus the reading done inside the file
/// in hand, rather than as records checked against the weight of every file. A
/// record's bytes are not its file's bytes: footers are never read as records,
/// a faulted record is read and counted nowhere, and a preallocated segment
/// reserves extents no record will ever sit in. Counted the other way a bar
/// stalls short of its end and every estimate drawn off it runs long.
struct Watch<'a> {
    swept: Swept,

    /// Weight of the files already finished, which progress never falls below
    finished: u64,

    /// Records checked since the watcher was last told, which paces the telling
    since_tell: u32,

    tell: &'a mut dyn FnMut(&Swept),
}

impl Watch<'_> {
    /// Records between one telling and the next
    ///
    /// A sweep checks millions of records and a watcher that repaints on each
    /// would cost more than the reading does, so the count is batched here where
    /// the loop is rather than left for every frontend to rediscover.
    const STRIDE: u32 = 64;

    /// One record of this many bytes checked, inside the file in hand
    fn record(&mut self, bytes: u64) {
        self.swept.bytes_done += bytes;
        self.since_tell += 1;
        if self.since_tell >= Watch::STRIDE {
            self.say();
        }
    }

    /// One segment file finished, whatever it held
    ///
    /// Progress snaps to the file's whole weight here, so the bar arrives at its
    /// end exactly once the last file is done however little of it was records.
    fn segment_done(&mut self, weight: u64) {
        self.finished += weight;
        self.swept.segments_done += 1;
        self.swept.bytes_done = self.finished;
        self.say();
    }

    fn say(&mut self) {
        self.since_tell = 0;
        (self.tell)(&self.swept);
    }
}

/// One segment file on disk, and what it weighs before anything reads it
struct SegmentFile {
    /// Segment number, taken from the file's name
    segment: SegmentId,

    /// Where the file sits, which root and all
    path: PathBuf,

    /// Bytes the file occupies, which is what a sweep has to work through
    ///
    /// Read from the directory entry rather than from the sweep, so a progress
    /// bar has a denominator before the first record is checked.
    weight: u64,
}

/// Every segment file on the volume's roots, in segment order
fn segment_files(roots: &[PathBuf]) -> Vec<SegmentFile> {
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
                files.push(SegmentFile {
                    segment: SegmentId(number),
                    path: entry.path(),
                    weight: entry.metadata().map(|data| data.len()).unwrap_or(0),
                });
            }
        }
    }
    files.sort_by_key(|file| file.segment);
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
fn sweep(engine: &ReelStore, file: &SegmentFile, indexed: bool, watch: &mut Watch) -> VerifyRow {
    let segment = file.segment;
    let path: &Path = &file.path;
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
            sweep_footer(&mut file, &footer, &mut row, watch);
        }
        Ok(None) => walk(&mut file, len, &mut row, watch),
        Err(error) => row.fault(format!("footer does not parse: {error}")),
    }
    row
}

/// Check every record a footer indexes, in the order they sit on disk
fn sweep_footer(file: &mut File, footer: &SegmentFooter, row: &mut VerifyRow, watch: &mut Watch) {
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
            // A footer names records rather than the header, but one that points
            // there is answered rather than skipped.
            Checked::Sound(bytes) | Checked::Header(bytes) => {
                row.sound(bytes);
                watch.record(bytes);
            }
            Checked::Fault(why) => row.fault(why),
            // A footer named the record, so unwritten space where it pointed is
            // the pointer being wrong rather than the end of anything.
            Checked::Frontier => row.fault(format!("record at {offset} is unwritten space")),
        }
    }
}

/// Walk a segment with no footer, record by record, up to its write frontier
fn walk(file: &mut File, len: u64, row: &mut VerifyRow, watch: &mut Watch) {
    let mut at = 0u64;
    while at + HEADER_LEN as u64 <= len {
        match check(file, at, (HEADER_LEN + MAX_KEY_LEN) as u64) {
            Checked::Sound(bytes) => {
                row.sound(bytes);
                watch.record(bytes);
                at += bytes;
            }
            // Read and checked like anything else, then stepped over: it is what
            // the segment is, not something written into it.
            Checked::Header(bytes) => {
                watch.record(bytes);
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

    /// The header a segment opens with, which spans bytes but is no record of its own
    Header(u64),

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
        true => match header.flags.is_segment_header() {
            true => Checked::Header(header.span()),
            false => Checked::Sound(header.span()),
        },
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

impl Report for VerifyReport {
    fn doc(&self) -> Doc {
        Doc::new()
            .head(fmt::volume_name(&self.volume))
            .head("verify")
            .head(fmt::plural(
                self.segments_swept as u64,
                "segment file",
                "segment files",
            ))
            .head("read-only, nothing written")
            .verdict(
                match self.is_sound() {
                    true => Tone::Good,
                    false => Tone::Bad,
                },
                match self.is_sound() {
                    true => "CLEAN",
                    false => "FAULTED",
                },
                self.detail(),
            )
            .facts([
                ("volume".to_string(), self.volume.clone()),
                ("records checked".to_string(), self.records.to_string()),
                ("bytes checked".to_string(), fmt::bytes(self.bytes)),
                ("carried rows".to_string(), self.carried_rows.to_string()),
            ])
            .notes("faults", Tone::Bad, self.faults())
            .table(self.segment_table())
            .notes(
                "not accounted for",
                Tone::Warn,
                caveat::notes(&self.caveats),
            )
            .term(
                "checked",
                [
                    "every sealed segment's footer decodes",
                    "every record a footer indexes matches its checksum",
                    "a segment with no footer is walked to its write frontier",
                    "every segment file on the roots holding a record is one the index names",
                ],
            )
            .term(
                "not checked",
                [
                    "versions a footer no longer indexes",
                    "whether the segments agree with each other",
                ],
            )
            .footer([match self.is_sound() {
                true => "machine-readable: -o json",
                false => "nothing was repaired: -o json for the faults as data",
            }])
    }
}

impl VerifyReport {
    /// The figures behind the verdict, which differ by what the verdict is
    fn detail(&self) -> String {
        match self.is_sound() {
            true => match self.records {
                0 => "nothing to check".to_string(),
                records => format!("{} records, {}, no faults", records, fmt::bytes(self.bytes)),
            },
            false => format!(
                "{} across {}, {} records checked",
                fmt::plural(self.faults, "fault", "faults"),
                fmt::plural(self.faulted_segments as u64, "segment", "segments"),
                self.records,
            ),
        }
    }

    /// The first fault of each faulted segment, which is the one worth naming
    ///
    /// One segment can fault many times over and the first is the one that says
    /// what went wrong; the rest are usually the same thing again. The others are
    /// counted rather than listed, so the block stays readable without the count
    /// in the verdict looking like it came from nowhere.
    fn faults(&self) -> Vec<Note> {
        let mut notes: Vec<Note> = self
            .segments
            .iter()
            .filter_map(|row| {
                let why = row.fault.as_ref()?;
                Some(Note::new(match row.faults {
                    0 | 1 => format!("segment {}: {why}", row.segment),
                    faults => format!(
                        "segment {}: {why} ({} more in the same segment)",
                        row.segment,
                        faults - 1,
                    ),
                }))
            })
            .collect();
        // The listing is capped, and a cap that hides faults without saying so
        // is the one thing a sweep must never do.
        if self.faulted_segments > notes.len() {
            notes.push(Note::new(format!(
                "{} further faulted segments the listing does not reach — --limit 0 for all",
                self.faulted_segments - notes.len(),
            )));
        }
        notes
    }

    /// The per-segment listing, with the files holding nothing counted rather
    /// than listed
    ///
    /// A volume seals segments whose every record has since been superseded, and
    /// a row of zeroes for each of them buries the rows that carry something. A
    /// faulted segment is never one of those, so nothing that matters is dropped
    /// and the caption says how many were.
    fn segment_table(&self) -> Table {
        let mut table = Table::new([
            Column::left("segment"),
            Column::left("kind"),
            Column::right("records"),
            Column::right("bytes"),
            Column::right("faults"),
        ]);
        let mut listed = 0usize;
        for row in &self.segments {
            if row.is_empty() {
                continue;
            }
            listed += 1;
            let cells = Row::new([
                row.segment.to_string(),
                match row.sealed {
                    true => "sealed".to_string(),
                    false => "walked".to_string(),
                },
                row.records.to_string(),
                fmt::bytes(row.bytes),
                row.faults.to_string(),
            ]);
            table = table.row(match row.faults {
                0 => cells,
                _ => cells.toned(Tone::Bad).note("faulted"),
            });
        }
        let mut caption = match listed == self.holding_segments {
            true => format!(
                "all {}",
                fmt::plural(
                    listed as u64,
                    "segment holding records",
                    "segments holding records"
                )
            ),
            false => format!(
                "{listed} of {} segments holding records — --limit 0 for all",
                self.holding_segments,
            ),
        };
        // Never silently: a file that swept clean and empty is still a file that
        // was read, and the count says so even though the row does not. It points
        // at --limit 0 and not at -o json, because the limit truncates the rows
        // before either format sees them and the empty ones are dropped first.
        if self.empty_segments > 0 {
            caption.push_str(&format!(
                "; {} swept clean and empty — --limit 0 to list them",
                self.empty_segments,
            ));
        }
        table.caption(caption)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::report::doc::Block;

    /// One segment's row, as a sweep would have left it
    fn row(segment: u32, records: u64, faults: u64) -> VerifyRow {
        VerifyRow {
            segment,
            sealed: true,
            indexed: true,
            records,
            bytes: records * 1024,
            carried: 0,
            faults,
            fault: (faults > 0).then(|| format!("record at {segment} fails its checksum")),
        }
    }

    /// A report over these rows, listing at most `limit` of them
    ///
    /// Built the way `verify` builds one, so the totals stand over every row and
    /// only the listing is cut.
    fn swept(mut rows: Vec<VerifyRow>, limit: usize) -> VerifyReport {
        let empty_segments = rows.iter().filter(|row| row.is_empty()).count();
        let report = VerifyReport {
            volume: "/srv/vol".to_string(),
            segments_swept: rows.len(),
            records: rows.iter().map(|row| row.records).sum(),
            bytes: rows.iter().map(|row| row.bytes).sum(),
            carried_rows: 0,
            faults: rows.iter().map(|row| row.faults).sum(),
            unreadable_records: 0,
            not_indexed: Vec::new(),
            empty_segments,
            holding_segments: rows.len() - empty_segments,
            faulted_segments: rows.iter().filter(|row| row.faults > 0).count(),
            caveats: Vec::new(),
            segments: {
                rows.sort_by(|a, b| {
                    b.faults
                        .cmp(&a.faults)
                        .then(a.is_empty().cmp(&b.is_empty()))
                        .then(a.segment.cmp(&b.segment))
                });
                rows.truncate(limit);
                rows
            },
        };
        report
    }

    /// The text of every note the report renders, block label and all
    fn notes(report: &VerifyReport) -> Vec<String> {
        report
            .doc()
            .blocks
            .iter()
            .filter_map(|block| match block {
                Block::Notes(notes) => Some(notes),
                _ => None,
            })
            .flat_map(|notes| notes.items.iter())
            .map(|note| note.what.clone())
            .collect()
    }

    /// What the segment table says it is a listing out of
    fn caption(report: &VerifyReport) -> String {
        report
            .doc()
            .blocks
            .iter()
            .find_map(|block| match block {
                Block::Table(table) => table.caption.clone(),
                _ => None,
            })
            .expect("a segment table")
    }

    // the verdict counts every faulted segment, not every listed one
    #[test]
    fn a_capped_listing_does_not_cap_the_verdict() {
        let rows: Vec<VerifyRow> = (1..=32).map(|at| row(at, 9, 1)).collect();
        let report = swept(rows, 20);
        let verdict = report.doc().verdict.expect("a verdict").detail;
        assert!(
            verdict.contains("32 faults across 32 segments"),
            "the verdict counted the listing rather than the sweep: {verdict}",
        );
    }

    // a listing that cannot reach every fault says how many it left
    #[test]
    fn a_capped_listing_says_what_it_left() {
        let rows: Vec<VerifyRow> = (1..=32).map(|at| row(at, 9, 1)).collect();
        let notes = notes(&swept(rows, 20));
        assert!(
            notes.iter().any(|note| note.contains("12 further faulted")),
            "the twelve faults past the cap went unsaid: {notes:?}",
        );
    }

    // an uncapped listing has nothing left to say
    #[test]
    fn an_uncapped_listing_says_nothing_extra() {
        let rows: Vec<VerifyRow> = (1..=4).map(|at| row(at, 9, 1)).collect();
        let notes = notes(&swept(rows, 20));
        assert_eq!(notes.len(), 4, "an invented remainder: {notes:?}");
    }

    // the empty segments are pointed at where they can actually be found
    #[test]
    fn empty_segments_are_pointed_at_a_listing_that_holds_them() {
        // Empties sort last and so are the first rows a cap drops, which is why
        // the caption cannot send a reader to the serialised rows for them.
        let mut rows: Vec<VerifyRow> = (1..=6).map(|at| row(at, 9, 0)).collect();
        rows.extend((7..=13).map(|at| row(at, 0, 0)));
        let report = swept(rows, 20);
        assert_eq!(report.empty_segments, 7, "the empties were miscounted");
        let caption = caption(&report);
        assert!(
            caption.contains("7 swept clean and empty"),
            "the empties went uncounted: {caption}",
        );
        assert!(
            !caption.contains("-o json"),
            "a listing the cap drops first is not where they are: {caption}",
        );
        assert!(
            caption.contains("--limit 0"),
            "no way given to reach them: {caption}",
        );
    }

    // progress is files finished plus the reading inside the file in hand
    #[test]
    fn progress_reaches_its_end() {
        // Records never account for a whole file: footers are not records, and a
        // preallocated segment reserves extents no record will sit in.
        let mut swept = Swept {
            segments_done: 0,
            segments_total: 2,
            bytes_done: 0,
            bytes_total: 200,
        };
        swept.bytes_done = 40;
        assert!(swept.fraction() < 0.5, "a part-read file cannot be half");
        swept.segments_done = 1;
        swept.bytes_done = 100;
        assert_eq!(swept.fraction(), 0.5, "a finished file is its whole weight");
        swept.segments_done = 2;
        swept.bytes_done = 200;
        assert_eq!(
            swept.fraction(),
            1.0,
            "the last file did not finish the bar"
        );
    }

    // a volume of empty files still has a fraction to draw
    #[test]
    fn progress_falls_back_to_counting_files() {
        let swept = Swept {
            segments_done: 1,
            segments_total: 4,
            bytes_done: 0,
            bytes_total: 0,
        };
        assert_eq!(swept.fraction(), 0.25);
    }
}
