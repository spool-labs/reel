//! Rebuilding the reel's resident index from its segment files on open
//!
//! The files are the truth and the index is a cache, so on open the reel is read
//! back from disk. A sealed segment is read from the packed sorted footer at its end;
//! an unsealed tail is walked once, ending at the first byte that cannot begin a
//! record. Every walked record is checksum verified and the ones that fail are left
//! out. A batch is read through the frame that opens it and is kept only when the run
//! the frame declared is there whole. A file whose segment header names an unknown
//! format or another segment number is quarantined rather than truncated. Each key
//! resolves to its highest sequence number, whichever segment carried it.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::path::{Path, PathBuf};

use crate::error::Result;
use crate::format::column::{ColumnId, KeyBytes, RecordKey};
use crate::format::footer::{
    FooterEntry, FooterPartition, FooterTally, SegmentFooter, FIXED_TAIL_LEN,
};
use crate::format::loc::{Loc, SegmentId};
use crate::format::lsn::Lsn;
use crate::format::record::{
    peek_key_width, read_u32_le, BatchFrame, Flags, RecordHeader, HEADER_LEN,
};
use crate::format::segment_header::{SegmentHeader, FORMAT_VERSION};
use crate::index::counters::SegmentBytes;
use crate::index::entry::{span_of, Entry, RangeCover};
use crate::index::persisted::{trusted, PersistedReader, PersistedSegment};
use crate::index::sealed_keys::SealedKeys;
use crate::io::op::FileId;
use crate::reel::segment::{IoDriver, SegmentReader};
use crate::reel::segment_number;

/// Bytes at the very end of a sealed segment holding its footer length and magic
const TRAILER_LEN: u64 = 8;

/// Bytes of the file end searched for the trailer past any aligned-write zeros
const TRAILER_PROBE_LEN: u64 = 4096;

/// One record recovered from a segment, before newest-wins resolution
struct SeenRecord {
    lsn: Lsn,
    segment: SegmentId,
    offset: u32,
    len: u32,
    key_width: u16,
    is_tombstone: bool,
}

impl SeenRecord {
    fn span(&self) -> u64 {
        span_of(self.key_width, self.len)
    }
}

/// The resident state a reel rebuild produces
pub struct RebuiltReel {
    /// Live winners per column, ready to install into the index
    pub entries: HashMap<ColumnId, Vec<(KeyBytes, Entry)>>,

    /// Ranges a delete swept that the index has to keep holding, empty when resident
    pub covers: Vec<RangeCover>,

    /// Per-segment live and dead byte footprints
    pub segments: HashMap<SegmentId, SegmentBytes>,

    /// Oldest data record sequence number each segment can still surface
    pub segment_min_lsn: HashMap<SegmentId, Lsn>,

    /// Newest row each sealed segment's footer holds, which is where a fan-out stops
    pub segment_max_lsn: HashMap<SegmentId, Lsn>,

    /// Key spans of the sealed segments a paged column answers from, empty when
    /// resident
    pub sealed: Vec<SealedSpan>,

    /// Highest sequence number seen, for reinitializing the counter
    pub highest_lsn: Lsn,

    /// Highest segment number present, for continuing the numbering
    pub highest_segment: SegmentId,

    /// Every sealed key per column, ready to stand ahead of the footer search
    pub sealed_keys: HashMap<ColumnId, SealedKeys>,

    /// Which root holds each segment found off the first volume, exceptions only
    pub placements: Vec<(SegmentId, u8)>,

    /// Files set aside as foreign or misplaced
    pub quarantined: Vec<PathBuf>,

    /// Bytes of each segment the rebuild consumed, carried only for the tails it
    /// walked
    pub consumed: HashMap<SegmentId, u64>,

    /// Walked tails and their file lengths. A crash keeps their reservation's
    /// blocks claimed past the end, and a writable open gives those back.
    pub walked: Vec<(PathBuf, u64)>,

    /// The same tails as appenders can pick them up, lowest number first
    pub resumable: Vec<ResumableTail>,
}

/// One sealed segment's key span for one column, which rules it in or out of a search
pub struct SealedSpan {
    pub column: ColumnId,
    pub segment: SegmentId,
    pub lowest: KeyBytes,
    pub highest: KeyBytes,
}

/// Rebuild the reel's index by reading every segment file in its directory
///
/// A paging volume sweeps its sealed segments rather than resolving them, so the peak
/// is one footer instead of one key set. Only the tails are resolved into memory.
pub fn rebuild_reel(
    driver: &IoDriver,
    roots: &[PathBuf],
    dead: &[bool],
    pages: bool,
) -> Result<RebuiltReel> {
    rebuild_from_persisted(driver, roots, dead, pages, None)
}

/// The same rebuild, offered an index a previous cue wrote down
///
/// The file speaks only for the segments still standing at the length it recorded,
/// and those are the only ones this skips reading. Everything else is read as it
/// would be with no file at all, and the two sets join through the same newest-wins
/// tournament, so a stale file can never be the reason a version is missed.
///
/// A paging volume takes no offer, since its sealed keys stay in their footers.
pub fn rebuild_from_persisted(
    driver: &IoDriver,
    roots: &[PathBuf],
    dead: &[bool],
    pages: bool,
    persisted: Option<PersistedReader>,
) -> Result<RebuiltReel> {
    let persisted = match pages {
        true => {
            if let Some(reader) = persisted {
                reader.close(driver)?;
            }
            None
        }
        false => persisted,
    };
    let mut files: Vec<(u32, PathBuf, u64, u8)> = Vec::new();
    for (at, root) in roots.iter().enumerate() {
        // A drive the operator declared dead is never read, even when something
        // still answers at its mountpoint.
        if dead.get(at).copied().unwrap_or(false) {
            continue;
        }
        for entry in driver.list_or_empty(root)? {
            if let Some(number) = segment_number(&entry.name) {
                files.push((number, root.join(&entry.name), entry.len, at as u8));
            }
        }
    }
    files.sort_by_key(|file| (file.0, file.3));
    // One id on two volumes is the store disagreeing with itself, and guessing which
    // file wins is how a stale copy shadows a live one. Refused, naming both.
    for pair in files.windows(2) {
        if pair[0].0 == pair[1].0 {
            return Err(crate::error::ReelError::Corruption(format!(
                "segment {} stands on two volumes, {} and {}",
                pair[0].0,
                pair[0].1.display(),
                pair[1].1.display(),
            )));
        }
    }

    // Read before anything is loaded, since what it names is what this open does not
    // have to read at all. The rows are read here and joined at the end, so a file
    // that goes bad costs only the set of segments it vouched for, and emptying that
    // set is the whole of the fallback.
    let mut standing = BTreeSet::new();
    let mut adopted = Adopted::default();
    if let Some(reader) = persisted {
        let present: HashMap<SegmentId, u64> = files
            .iter()
            .map(|(number, _, len, _)| (SegmentId(*number), *len))
            .collect();
        standing = trusted(&reader.index, &present);
        match read_adopted(driver, reader, &standing) {
            Ok(rows) => adopted = rows,
            Err(error) => {
                tracing::warn!(
                    "the persisted index went bad partway through its rows, so this open \
                     sweeps the footers: {error}",
                );
                standing.clear();
            }
        }
    }

    let mut resolver = Resolver::new(pages);
    let mut quarantined = Vec::new();
    let mut consumed = HashMap::new();
    let mut walked = Vec::new();
    let mut resumable: Vec<ResumableTail> = Vec::new();
    let mut sealed_files = Vec::new();
    let mut placements = Vec::new();
    let mut highest_number = 0u32;
    for (number, path, len, root) in files {
        highest_number = highest_number.max(number);
        let segment = SegmentId(number);
        if root != 0 {
            placements.push((segment, root));
        }
        if standing.contains(&segment) {
            // Sealed and vouched for, so the file is read to its end and the rows
            // below stand in for walking it.
            consumed.insert(segment, len);
            continue;
        }
        match load_segment(driver, &path, segment, len, pages, &mut resolver)? {
            Loaded::Sealed => {
                consumed.insert(segment, len);
                sealed_files.push((segment, path, len));
            }
            Loaded::Walked(offset, rows) => {
                consumed.insert(segment, offset);
                walked.push((path.clone(), len));
                // Resumable means the file ends at its records. A file running
                // past its walk holds bytes no appender may write behind.
                if offset == len {
                    resumable.push(ResumableTail {
                        segment,
                        path,
                        end: offset,
                        rows,
                    });
                }
            }
            Loaded::Foreign => quarantined.push(path),
        }
    }
    adopted.join(&mut resolver);

    let mut resolved = resolver.finish();
    if pages {
        prune_walked_shadowed(driver, &sealed_files, &mut resolved)?;
    }
    Ok(RebuiltReel {
        entries: resolved.entries,
        covers: resolved.covers,
        segments: resolved.segments,
        segment_min_lsn: resolved.segment_min_lsn,
        segment_max_lsn: resolved.segment_max_lsn,
        sealed: resolved.sealed,
        highest_lsn: resolved.highest,
        highest_segment: SegmentId(highest_number),
        sealed_keys: resolved.sealed_keys,
        placements,
        quarantined,
        consumed,
        walked,
        resumable: {
            resumable.sort_by_key(|tail| tail.segment.as_u32());
            resumable
        },
    })
}

/// A persisted index's rows and counters, read but not yet joined
///
/// Held apart from the resolver so the segment sweep decides the order the runs go
/// in, which is the order a tie between a row and a segment falls in.
#[derive(Default)]
struct Adopted {
    /// Sequence number the file stood at
    at: Lsn,

    /// Stamps of the segments the file still speaks for
    stamps: Vec<PersistedSegment>,

    /// One run a column, in the order the file wrote them
    runs: Vec<Vec<(RecordKey, SeenRecord)>>,
}

impl Adopted {
    /// Fold the rows and counters into the join
    ///
    /// The rows go in as one run a column, the shape the tournament already takes from
    /// a footer, so a version written after the checkpoint outversions its row. The
    /// file holds only winners, so a segment's shadowed and tombstoned bytes come off
    /// the stamps instead. The sequence number the checkpoint stood at is taken as a
    /// floor whether or not any segment survived it, since a reissued number over a
    /// version some file still holds is a resurrection.
    fn join(self, resolver: &mut Resolver) {
        for stamp in &self.stamps {
            resolver.adopt_segment(stamp);
        }
        for run in self.runs {
            resolver.absorb_sorted_run(run);
        }
        if self.at > resolver.highest {
            resolver.highest = self.at;
        }
    }
}

/// Read a persisted index's rows off the medium, vouched segments only
///
/// One run a column and one copy a key, taken straight off the block the reader is
/// holding. The reader refuses a file whose keys do not rise, which is what lets each
/// run go in sorted.
fn read_adopted(
    driver: &IoDriver,
    mut reader: PersistedReader,
    standing: &BTreeSet<SegmentId>,
) -> Result<Adopted> {
    let mut adopted = Adopted {
        at: reader.index.at,
        stamps: Vec::new(),
        runs: Vec::new(),
    };
    for stamp in &reader.index.segments {
        if standing.contains(&stamp.segment) {
            adopted.stamps.push(*stamp);
        }
    }
    let read = (|| -> Result<()> {
        let mut column: Option<ColumnId> = None;
        let mut run: Vec<(RecordKey, SeenRecord)> = Vec::new();
        while reader.advance(driver)? {
            if column != Some(reader.column()) {
                adopted.runs.push(std::mem::take(&mut run));
                column = Some(reader.column());
            }
            let loc = reader.loc();
            if !standing.contains(&loc.segment) {
                continue;
            }
            let key = RecordKey::from_bytes(reader.column(), reader.key())?;
            let key_width = key.width();
            run.push((
                key,
                SeenRecord {
                    lsn: reader.lsn(),
                    segment: loc.segment,
                    offset: loc.offset,
                    len: loc.len,
                    key_width,
                    is_tombstone: false,
                },
            ));
        }
        adopted.runs.push(run);
        Ok(())
    })();
    reader.close(driver)?;
    read?;
    Ok(adopted)
}

/// What reading one segment during a rebuild turned out to be
enum Loaded {
    /// A sealed segment, read from its footer, with nothing left to follow
    Sealed,

    /// An unsealed tail, walked to an offset, its rows in walk order
    Walked(u64, Vec<FooterEntry>),

    /// A file that is not a segment of this reel
    Foreign,
}

/// An unsealed tail an appender can pick up where it stopped
///
/// The end is the walked offset, and the rows are what the tail's in-memory
/// footer held when the process went: rebuilt from the walk, carrying nothing
/// inline, so a read through one goes to the record.
pub struct ResumableTail {
    pub segment: SegmentId,
    pub path: PathBuf,
    pub end: u64,
    pub rows: Vec<FooterEntry>,
}

/// Read one segment into the resolver, releasing its descriptor either way
///
/// A rebuild opens every segment file, so a descriptor left behind here is one per
/// segment on every open of the volume.
fn load_segment(
    driver: &IoDriver,
    path: &Path,
    segment: SegmentId,
    file_len: u64,
    pages: bool,
    resolver: &mut Resolver,
) -> Result<Loaded> {
    let file = driver.open(path, false)?;
    let loaded = read_segment(driver, file, segment, file_len, pages, resolver);
    driver.close(file)?;
    loaded
}

fn read_segment(
    driver: &IoDriver,
    file: FileId,
    segment: SegmentId,
    file_len: u64,
    pages: bool,
    resolver: &mut Resolver,
) -> Result<Loaded> {
    if !belongs_here(driver, file, segment)? {
        return Ok(Loaded::Foreign);
    }

    match read_footer(driver, file, file_len)? {
        Some(footer) => {
            match pages {
                true => sweep_footer(driver, file, segment, &footer, resolver)?,
                false => collect_from_footer(driver, file, segment, &footer, resolver)?,
            }
            Ok(Loaded::Sealed)
        }
        None => {
            let mut reader = SegmentReader::new(driver, file, file_len);
            let walked = walk_records(&mut reader, segment, 0, file_len)?;
            let reached = walked.next_offset;
            let rows = walked
                .records
                .iter()
                .map(|record| {
                    FooterEntry::new(
                        record.key.clone(),
                        record.lsn,
                        record.loc.offset,
                        record.loc.len,
                        record.flags,
                    )
                })
                .collect();
            absorb_walked(resolver, walked.records);
            Ok(Loaded::Walked(reached, rows))
        }
    }
}

/// Drop walked entries a sealed footer outversions, which paged installs must not hold
///
/// A walked entry installs as its key's newest and shadows the footer search, which
/// is right for a tail and wrong for a segment whose seal failed. So every walked
/// entry is checked against the sealed footers whose span admits its key and dropped
/// where a newer version stands; one newer than every sealed row cannot lose that
/// comparison, which exempts a tail's fresh writes without leaning on segment
/// numbers. The other direction is owed too: a surviving walked entry books the
/// newest sealed data row it shadows dead, since the sealed tally froze at the seal.
fn prune_walked_shadowed(
    driver: &IoDriver,
    sealed_files: &[(SegmentId, PathBuf, u64)],
    resolved: &mut ResolvedRecords,
) -> Result<()> {
    if sealed_files.is_empty() {
        return Ok(());
    }
    let ResolvedRecords {
        entries,
        segments,
        sealed,
        ..
    } = resolved;

    // Every walked entry joins, per column, in key order: one at or below the newest
    // sealed row could be outversioned, and any could be shadowing an older one.
    let mut suspects: HashMap<ColumnId, Vec<usize>> = HashMap::new();
    for (column, rows) in entries.iter() {
        if !rows.is_empty() {
            suspects.insert(*column, (0..rows.len()).collect());
        }
    }
    if suspects.is_empty() {
        return Ok(());
    }

    // Only the segments whose span admits a suspect key are worth reopening.
    let mut probe: HashMap<SegmentId, Vec<ColumnId>> = HashMap::new();
    for span in sealed.iter() {
        let Some(wanted) = suspects.get(&span.column) else {
            continue;
        };
        let rows = &entries[&span.column];
        let admits = wanted.iter().any(|at| {
            let key = rows[*at].0.as_slice();
            span.lowest.as_slice() <= key && key <= span.highest.as_slice()
        });
        if admits {
            let columns = probe.entry(span.segment).or_default();
            if !columns.contains(&span.column) {
                columns.push(span.column);
            }
        }
    }

    let mut shadowed: HashMap<ColumnId, Vec<usize>> = HashMap::new();
    // The newest sealed data row each walked entry shadows, across every footer
    // that holds its key, booked once the prune has said the entry survives.
    let mut debits: HashMap<ColumnId, HashMap<usize, (SegmentId, Lsn, u64)>> = HashMap::new();
    for (segment, path, len) in sealed_files {
        let Some(columns) = probe.get(segment) else {
            continue;
        };
        let file = driver.open(path, false)?;
        let outcome: Result<()> = (|| {
            let Some(footer) = read_footer(driver, file, *len)? else {
                return Ok(());
            };
            for partition in &footer.partitions {
                if !columns.contains(&partition.column) {
                    continue;
                }
                let wanted = &suspects[&partition.column];
                let rows = &entries[&partition.column];
                let marks = shadowed.entry(partition.column).or_default();
                // The footer rows and the suspects are both in key order, so
                // one forward pass joins them.
                let mut at = 0usize;
                for row in partition.entries() {
                    let row = row?;
                    let key = row.key.as_slice();
                    while at < wanted.len() && rows[wanted[at]].0.as_slice() < key {
                        at += 1;
                    }
                    if at == wanted.len() {
                        break;
                    }
                    let (suspect, entry) = &rows[wanted[at]];
                    if suspect.as_slice() != key {
                        continue;
                    }
                    if row.lsn > entry.lsn {
                        marks.push(wanted[at]);
                    } else if row.lsn < entry.lsn
                        && entry.lsn >= footer.sealed_at
                        && !row.is_tombstone()
                        && !row.is_range_tombstone()
                    {
                        // At or past the frontier, so the tally froze without this
                        // shadowing. Below it the tally already counted the death and
                        // a debit here would count it twice.
                        let span = span_of(row.key.width(), row.len);
                        let held = debits
                            .entry(partition.column)
                            .or_default()
                            .entry(wanted[at])
                            .or_insert((*segment, row.lsn, span));
                        if row.lsn > held.1 {
                            *held = (*segment, row.lsn, span);
                        }
                    }
                }
            }
            Ok(())
        })();
        driver.close(file)?;
        outcome?;
    }

    // A walked entry the prune drops lost to a sealed row, so that row is live
    // and owes no debit; the debit belongs only to the entries that survive.
    for (column, walked) in debits {
        let pruned: HashSet<usize> = shadowed
            .get(&column)
            .map(|marks| marks.iter().copied().collect())
            .unwrap_or_default();
        for (at, (segment, _lsn, span)) in walked {
            if pruned.contains(&at) {
                continue;
            }
            let row = segments.entry(segment).or_default();
            row.live = row.live.saturating_sub(span);
            row.dead += span;
        }
    }

    // Take the shadowed entries out, booking a dropped record dead where it lies. A
    // dropped grave keeps its tombstone booking, which is bytes the segment holds.
    for (column, mut marks) in shadowed {
        marks.sort_unstable();
        marks.dedup();
        let rows = entries.get_mut(&column).expect("a marked column");
        for at in marks.iter().rev() {
            let (key, entry) = rows.remove(*at);
            if !entry.is_grave() {
                let row = segments.entry(entry.loc.segment).or_default();
                let span = span_of(key.width(), entry.loc.len);
                row.live = row.live.saturating_sub(span);
                row.dead += span;
            }
        }
    }
    Ok(())
}

/// Whether this file is a segment of this reel, written under this format
fn belongs_here(driver: &IoDriver, file: FileId, segment: SegmentId) -> Result<bool> {
    let head_bytes = driver.pread(file, 0, HEADER_LEN as u64)?;
    if head_bytes.len() < HEADER_LEN {
        return Ok(false);
    }
    let header = match RecordHeader::unpack(&head_bytes) {
        Ok(header) => header,
        Err(_) => return Ok(false),
    };
    if !header.flags.is_segment_header() {
        return Ok(false);
    }

    let payload = driver.pread(file, HEADER_LEN as u64, u64::from(header.length))?;
    if payload.len() < header.length as usize || !header.verify(&payload) {
        return Ok(false);
    }
    match SegmentHeader::unpack(&payload) {
        Ok(parsed) => Ok(parsed.segment == segment && parsed.version == FORMAT_VERSION),
        Err(_) => Ok(false),
    }
}

/// Take a sealed segment's records from its footer, reading only its ranges back
///
/// Sealing waits for every reservation and syncs, so what a footer lists is what
/// landed and a batch frame has nothing left to decide. The one thing a footer
/// cannot answer is a range tombstone's end, so those are read back one at a time.
fn collect_from_footer(
    driver: &IoDriver,
    file: FileId,
    segment: SegmentId,
    footer: &SegmentFooter,
    resolver: &mut Resolver,
) -> Result<()> {
    collect_partitions(driver, file, segment, &footer.partitions, resolver)
}

/// Absorb every row of these partitions into the join
fn collect_partitions(
    driver: &IoDriver,
    file: FileId,
    segment: SegmentId,
    partitions: &[FooterPartition],
    resolver: &mut Resolver,
) -> Result<()> {
    // One run per source. The partitions come sorted by column and their rows by key,
    // which is the merge's own order, so the run pays no sort at all.
    let mut run = Vec::new();
    for entry in partitions.iter().flat_map(|partition| partition.entries()) {
        let entry = entry?;
        if entry.is_range_tombstone() {
            let end = read_range_end(driver, file, &entry.key, entry.offset, entry.len)?;
            let span = span_of(entry.key.width(), entry.len);
            resolver.absorb_range(
                RangeCover {
                    start: entry.key,
                    end,
                    lsn: entry.lsn,
                },
                segment,
                span,
            );
            continue;
        }
        let key_width = entry.key.width();
        let is_tombstone = entry.is_tombstone();
        run.push((
            entry.key,
            SeenRecord {
                lsn: entry.lsn,
                segment,
                offset: entry.offset,
                len: entry.len,
                key_width,
                is_tombstone,
            },
        ));
    }
    resolver.absorb_sorted_run(run);
    Ok(())
}

/// Tally one sealed segment's footer without installing any of its keys
///
/// The keys stay in the footer, so what a rebuild installs is the span, the oldest
/// record the segment could still surface, and the range tombstones, whose ends live
/// in payloads. The live and dead split comes from the tally written at the seal, and
/// the scrub settles shadowing after it. Live key counts are left alone, since a key
/// rewritten into several segments appears in several footers.
fn sweep_footer(
    driver: &IoDriver,
    file: FileId,
    segment: SegmentId,
    footer: &SegmentFooter,
    resolver: &mut Resolver,
) -> Result<()> {
    resolver.book_tally(segment, footer.tally);
    resolver.book_footer_max(segment, footer.max_lsn);
    for partition in &footer.partitions {
        if let Some((lowest, highest)) = partition.key_range() {
            resolver.span(SealedSpan {
                column: partition.column,
                segment,
                lowest: KeyBytes::new(lowest)?,
                highest: KeyBytes::new(highest)?,
            });
        }

        for entry in partition.entries() {
            let entry = entry?;
            // Fed before the span is installed, so a search the span admits is never
            // ruled out by a filter that has not heard of the segment.
            resolver
                .sealed_keys
                .entry(partition.column)
                .or_default()
                .insert(entry.key.as_slice());
            let span = span_of(entry.key.width(), entry.len);
            if entry.is_range_tombstone() {
                let end = read_range_end(driver, file, &entry.key, entry.offset, entry.len)?;
                resolver.absorb_range(
                    RangeCover {
                        start: entry.key,
                        end,
                        lsn: entry.lsn,
                    },
                    segment,
                    span,
                );
                continue;
            }
            match entry.is_tombstone() {
                true => resolver.book_sealed_tombstone(segment, entry.lsn, span),
                false => resolver.book_sealed(segment, entry.lsn),
            }
        }
    }
    Ok(())
}

/// Read the exclusive end a range tombstone carries as its payload
fn read_range_end(
    driver: &IoDriver,
    file: FileId,
    key: &RecordKey,
    offset: u32,
    len: u32,
) -> Result<Option<KeyBytes>> {
    if len == 0 {
        return Ok(None);
    }
    let at = u64::from(offset) + HEADER_LEN as u64 + u64::from(key.width());
    let bytes = driver.pread(file, at, u64::from(len))?;
    if bytes.len() < len as usize {
        return Ok(None);
    }
    Ok(KeyBytes::new(&bytes).ok())
}

/// One record a walk resolved, carrying everything applying it needs
pub struct WalkedRecord {
    /// Column and key the record is addressed by
    pub key: RecordKey,

    /// Sequence number that orders it
    pub lsn: Lsn,

    /// Where it sits on the volume
    pub loc: Loc,

    /// Control bits, which say what applying it means
    pub flags: Flags,

    /// Exclusive end of the range, for a range tombstone
    pub range_end: Option<KeyBytes>,
}

/// What one walk of a segment produced
pub struct Walked {
    /// Records the walk resolved, in the order they sit in the segment
    pub records: Vec<WalkedRecord>,

    /// Offset the walk stopped at, which is where the next one resumes
    pub next_offset: u64,
}

/// Walk a segment from an offset, vetting every record and framing every batch
///
/// The walk stops at the first byte that cannot begin a record and hands back that
/// offset, so a later walk of a tail that has grown picks up exactly there. A batch is
/// read as one thing: its frame says how many records follow and how many bytes they
/// take, and the run is kept only when exactly that is there and verifies. The offset
/// reported is the one before a run whose bytes have not all landed rather than past
/// it. Pass the file length to walk to the end.
pub fn walk_records(
    reader: &mut SegmentReader<'_>,
    segment: SegmentId,
    from: u64,
    to: u64,
) -> Result<Walked> {
    let limit = reader.limit().min(to);
    let mut records = Vec::new();
    // Where a walk may resume from. It trails the write head by whatever an unlanded
    // run holds, so a tail that grows into its own batch is re-read from the frame
    // rather than resumed inside it.
    let mut next_offset = from;
    let mut offset = from;
    while offset + HEADER_LEN as u64 <= limit {
        let header = match read_head(reader, offset, limit)? {
            Head::Record(header) => header,
            Head::Missing | Head::Broken => break,
        };
        if !header.fits_within(limit - offset) {
            break;
        }
        let at = offset;
        offset += header.span();

        if header.flags.is_batch_frame() {
            // The frame's own checksum covers what it declares, so a frame that fails
            // it leaves nothing saying where its batch ends and the walk stops here.
            if !verify_record(reader, at, &header)? {
                break;
            }
            let declaration = reader.range(at + header.prefix_len(), header.length as usize)?;
            let Some(frame) = BatchFrame::unpack(&header, declaration) else {
                break;
            };
            let ends_at = offset + frame.span;
            if ends_at > limit {
                break;
            }
            match walk_batch(reader, segment, &frame, offset, ends_at)? {
                Batch::Whole(run) => records.extend(run),
                // The run is on disk and not intact, so it is dropped whole and the
                // walk carries on at the boundary the frame named.
                Batch::Torn => {}
                // The bytes are not all there, so the tail may yet grow into them and
                // the walk leaves the frame for the pass that finds them.
                Batch::Unlanded => break,
            }
            offset = ends_at;
            next_offset = ends_at;
            continue;
        }

        // A member without its frame is a record of a run this walk cannot vouch for,
        // so it is dropped rather than applied on its own.
        if header.flags.is_batched() {
            next_offset = at + header.span();
            continue;
        }

        // A record that fails its checksum is dropped and the walk carries on, since
        // cutting the walk there would throw away every good record behind it.
        if !verify_record(reader, at, &header)? || !is_indexable(header.flags) {
            next_offset = at + header.span();
            continue;
        }
        let span = header.span();
        records.push(resolve_walked(reader, segment, header, at)?);
        next_offset = at + span;
    }

    Ok(Walked {
        records,
        next_offset,
    })
}

/// What the bytes at an offset turned out to be
enum Head {
    /// A record header, parsed and within the bytes the walk may read
    Record(RecordHeader),

    /// Bytes nothing has written yet, which is where a tail's reservation begins
    Missing,

    /// Bytes that are there and do not begin a record
    Broken,
}

/// Parse the record beginning at an offset, telling absent bytes from bad ones
///
/// The two are not the same answer: bytes that are not there may arrive, and bytes
/// that are there and will not parse never will.
fn read_head(reader: &mut SegmentReader<'_>, offset: u64, limit: u64) -> Result<Head> {
    if offset + HEADER_LEN as u64 > limit {
        return Ok(Head::Missing);
    }
    let head_bytes = reader.range(offset, HEADER_LEN)?;
    if head_bytes.len() < HEADER_LEN {
        return Ok(Head::Missing);
    }
    let Some(width) = peek_key_width(head_bytes) else {
        return Ok(Head::Broken);
    };
    let prefix = reader.range(offset, HEADER_LEN + width)?;
    if prefix.len() < HEADER_LEN + width {
        return Ok(Head::Missing);
    }
    let Ok(header) = RecordHeader::unpack(prefix) else {
        return Ok(Head::Broken);
    };
    match header.is_unwritten() {
        true => Ok(Head::Missing),
        false => Ok(Head::Record(header)),
    }
}

/// What one batch's declared region turned out to hold
enum Batch {
    /// Every record the frame declared is there and verifies
    Whole(Vec<WalkedRecord>),

    /// The region is written and is not the run the frame declared
    Torn,

    /// The region's bytes have not all landed, so the run may still be coming
    Unlanded,
}

/// Read the run one frame declares, keeping it only if all of it is there
///
/// Nothing is applied until the whole run has been read, so a batch is never half
/// installed and then retracted. Both of the frame's numbers have to come out: the
/// records are counted and the bytes they take have to end exactly where the span
/// said, so a run that stops early or runs long is torn either way.
fn walk_batch(
    reader: &mut SegmentReader<'_>,
    segment: SegmentId,
    frame: &BatchFrame,
    from: u64,
    ends_at: u64,
) -> Result<Batch> {
    let mut run = Vec::new();
    let mut offset = from;
    for _ in 0..frame.count {
        let header = match read_head(reader, offset, ends_at)? {
            Head::Record(header) => header,
            Head::Missing => return Ok(Batch::Unlanded),
            Head::Broken => return Ok(Batch::Torn),
        };
        // A record of a run says so in its own checksummed header, so a run holding
        // anything that does not is not the run the frame declared.
        if !header.flags.is_batched()
            || !is_indexable(header.flags)
            || !header.fits_within(ends_at - offset)
        {
            return Ok(Batch::Torn);
        }
        if !verify_record(reader, offset, &header)? {
            return Ok(Batch::Torn);
        }
        let at = offset;
        offset += header.span();
        run.push(resolve_walked(reader, segment, header, at)?);
    }
    match offset == ends_at {
        true => Ok(Batch::Whole(run)),
        false => Ok(Batch::Torn),
    }
}

/// Resolve one walked record, reading back the end a range tombstone carries
fn resolve_walked(
    reader: &mut SegmentReader<'_>,
    segment: SegmentId,
    header: RecordHeader,
    offset: u64,
) -> Result<WalkedRecord> {
    let range_end = match header.flags.is_range_tombstone() {
        false => None,
        true => match header.length {
            0 => None,
            length => {
                let bytes = reader.range(offset + header.prefix_len(), length as usize)?;
                KeyBytes::new(bytes).ok()
            }
        },
    };

    Ok(WalkedRecord {
        key: header.key,
        lsn: header.lsn,
        loc: Loc::new(segment, offset as u32, header.length),
        flags: header.flags,
        range_end,
    })
}

/// Fold a walk's records into the resolver, ranges apart from keys
///
/// The records arrive in file order, so the run is sorted into key order here.
fn absorb_walked(resolver: &mut Resolver, records: Vec<WalkedRecord>) {
    let mut run = Vec::with_capacity(records.len());
    for record in records {
        let key_width = record.key.width();
        if record.flags.is_range_tombstone() {
            let span = span_of(key_width, record.loc.len);
            resolver.absorb_range(
                RangeCover {
                    start: record.key,
                    end: record.range_end,
                    lsn: record.lsn,
                },
                record.loc.segment,
                span,
            );
            continue;
        }
        run.push((
            record.key,
            SeenRecord {
                lsn: record.lsn,
                segment: record.loc.segment,
                offset: record.loc.offset,
                len: record.loc.len,
                key_width,
                is_tombstone: record.flags.is_tombstone(),
            },
        ));
    }
    resolver.absorb_unsorted_run(run);
}

fn verify_record(
    reader: &mut SegmentReader<'_>,
    offset: u64,
    header: &RecordHeader,
) -> Result<bool> {
    if !header.has_payload() {
        return Ok(header.verify(&[]));
    }
    let length = header.length as usize;
    let payload = reader.range(offset + header.prefix_len(), length)?;
    if payload.len() < length {
        return Ok(false);
    }
    Ok(header.verify(payload))
}

/// What a resolver has left once every segment has been read into it
struct ResolvedRecords {
    entries: HashMap<ColumnId, Vec<(KeyBytes, Entry)>>,
    covers: Vec<RangeCover>,
    segments: HashMap<SegmentId, SegmentBytes>,
    segment_min_lsn: HashMap<SegmentId, Lsn>,
    segment_max_lsn: HashMap<SegmentId, Lsn>,
    sealed: Vec<SealedSpan>,
    sealed_keys: HashMap<ColumnId, SealedKeys>,
    highest: Lsn,
}

/// Newest-wins resolution over sorted runs, joined once at the finish
///
/// The sources are already sorted, so resolving them through a map would pay a
/// descent per record to rediscover an order the inputs had. Each run is folded to
/// one version a key as it arrives, and the runs are merged once through a loser
/// tree, which leaves the per-column output sorted for the bulk install.
struct Resolver {
    runs: Vec<Vec<(RecordKey, SeenRecord)>>,
    ranges: Vec<RangeCover>,
    segments: HashMap<SegmentId, SegmentBytes>,
    segment_min_lsn: HashMap<SegmentId, Lsn>,
    segment_max_lsn: HashMap<SegmentId, Lsn>,
    sealed: Vec<SealedSpan>,
    sealed_keys: HashMap<ColumnId, SealedKeys>,
    highest: Lsn,
    pages: bool,
}

impl Resolver {
    fn new(pages: bool) -> Resolver {
        Resolver {
            runs: Vec::new(),
            ranges: Vec::new(),
            segments: HashMap::new(),
            segment_min_lsn: HashMap::new(),
            segment_max_lsn: HashMap::new(),
            sealed: Vec::new(),
            sealed_keys: HashMap::new(),
            highest: Lsn::NONE,
            pages,
        }
    }

    /// Take one source's records as a run already in key order
    ///
    /// The bookkeeping that does not depend on the join happens here. Booking a
    /// tombstone's hold now is what keeps a segment of nothing but tombstones from
    /// having no row at all, which neither compaction nor the scrub could see.
    fn absorb_sorted_run(&mut self, mut run: Vec<(RecordKey, SeenRecord)>) {
        if run.is_empty() {
            return;
        }
        for (_, record) in &run {
            if record.lsn > self.highest {
                self.highest = record.lsn;
            }
            match record.is_tombstone {
                true => self.hold(record.segment, record.lsn, record.span()),
                false => note_segment_min(&mut self.segment_min_lsn, record.segment, record.lsn),
            }
        }
        self.fold_newest(&mut run);
        self.runs.push(run);
    }

    /// Cut a run down to one version a key, booking every version it drops dead
    ///
    /// The run is in key then sequence order, so a key's versions are adjacent and the
    /// last of them is the newest. The join would resolve them the same way and book the
    /// same losers dead, so doing it here holds a source at the size of the keys it still
    /// resolves rather than of every version it ever wrote.
    fn fold_newest(&mut self, run: &mut Vec<(RecordKey, SeenRecord)>) {
        let mut kept = 0usize;
        for at in 1..run.len() {
            if run[kept].0 != run[at].0 {
                kept += 1;
                run.swap(kept, at);
                continue;
            }
            // An exact tie falls to the version already held, which is what the join
            // does with a tie between two runs.
            match run[at].1.lsn > run[kept].1.lsn {
                true => {
                    self.book_dead(&run[kept].1);
                    run.swap(kept, at);
                }
                false => self.book_dead(&run[at].1),
            }
        }
        if kept + 1 < run.len() {
            run.truncate(kept + 1);
            run.shrink_to_fit();
        }
    }

    /// Take a walked tail's records, which arrive in file order rather than key
    fn absorb_unsorted_run(&mut self, mut run: Vec<(RecordKey, SeenRecord)>) {
        run.sort_unstable_by(|one, two| one.0.cmp(&two.0).then_with(|| one.1.lsn.cmp(&two.1.lsn)));
        self.absorb_sorted_run(run);
    }

    fn absorb_range(&mut self, range: RangeCover, segment: SegmentId, span: u64) {
        if range.lsn > self.highest {
            self.highest = range.lsn;
        }
        self.hold(segment, range.lsn, span);
        self.ranges.push(range);
    }

    /// Note a sealed segment's span for one column, which a paged read searches by
    fn span(&mut self, span: SealedSpan) {
        self.sealed.push(span);
    }

    /// Note the ceiling a sealed segment's footer puts on the rows it can answer with
    ///
    /// Off the footer's own bound rather than the rows the sweep books, since a
    /// tombstone row books no sequence number anywhere.
    fn book_footer_max(&mut self, segment: SegmentId, max: Lsn) {
        if max == Lsn::NONE {
            return;
        }
        let held = self.segment_max_lsn.entry(segment).or_insert(max);
        if max > *held {
            *held = max;
        }
    }

    /// Book what a sealed segment weighed when it closed
    ///
    /// The footprint comes from the segment rather than its rows, since the live and
    /// dead split is the one thing reading the rows cannot answer.
    fn book_tally(&mut self, segment: SegmentId, tally: FooterTally) {
        let row = self.segments.entry(segment).or_default();
        row.live += tally.live;
        row.dead += tally.dead;
    }

    /// Note a sealed record the sweep is leaving in its footer
    ///
    /// The bytes came from the tally, so what is left is the sequence number: a
    /// tombstone below this record has to be carried.
    fn book_sealed(&mut self, segment: SegmentId, lsn: Lsn) {
        if lsn > self.highest {
            self.highest = lsn;
        }
        note_segment_min(&mut self.segment_min_lsn, segment, lsn);
    }

    /// Note a sealed tombstone row, which holds space in the segment it sits in
    fn book_sealed_tombstone(&mut self, segment: SegmentId, lsn: Lsn, span: u64) {
        self.hold(segment, lsn, span);
    }

    /// Take a persisted index's counters for a segment it still speaks for
    ///
    /// Live starts at the tombstone hold rather than at what the counters called
    /// live, since the winners come back through the join and are added as they land.
    /// What is left is the part no row of the file carries: the tombstones, the bytes
    /// already shadowed, and the oldest record the segment can still surface.
    fn adopt_segment(&mut self, stamp: &PersistedSegment) {
        let row = self.segments.entry(stamp.segment).or_default();
        row.live += stamp.held;
        row.dead += stamp.dead;
        row.held += stamp.held;
        row.held_lsn = match (row.held_lsn, stamp.held_lsn) {
            (Some(held), Some(theirs)) => Some(held.max(theirs)),
            (held, theirs) => held.or(theirs),
        };
        if let Some(min) = stamp.min_lsn {
            note_segment_min(&mut self.segment_min_lsn, stamp.segment, min);
        }
    }

    /// Book a tombstone's footprint against the segment holding it
    fn hold(&mut self, segment: SegmentId, lsn: Lsn, span: u64) {
        if lsn > self.highest {
            self.highest = lsn;
        }
        let row = self.segments.entry(segment).or_default();
        row.live += span;
        row.held += span;
        row.held_lsn = Some(row.held_lsn.map_or(lsn, |newest| newest.max(lsn)));
    }

    fn book_dead(&mut self, record: &SeenRecord) {
        if record.is_tombstone {
            return;
        }
        self.segments.entry(record.segment).or_default().dead += record.span();
    }

    /// Resolve what survived into what the index will hold
    ///
    /// A paged rebuild carries its range covers, since no footer search can find a
    /// range delete at all: a range is written against its start alone.
    fn finish(mut self) -> ResolvedRecords {
        let mut entries: HashMap<ColumnId, Vec<(KeyBytes, Entry)>> = HashMap::new();

        // One tournament ordered by key, then sequence, then the run's own age, so a
        // tie falls to the earliest source. The winner of a key is resolved between
        // neighbours as they come out and every loser is booked dead where it lies.
        let mut merge = LoserTree::new(std::mem::take(&mut self.runs));

        let mut pending: Option<(RecordKey, SeenRecord)> = None;
        while let Some((key, record)) = merge.pop() {
            pending = Some(match pending.take() {
                None => (key, record),
                Some((held_key, held)) if held_key == key => match record.lsn > held.lsn {
                    true => {
                        self.book_dead(&held);
                        (key, record)
                    }
                    false => {
                        self.book_dead(&record);
                        (held_key, held)
                    }
                },
                Some(done) => {
                    self.settle(done, &mut entries);
                    (key, record)
                }
            });
        }
        if let Some(done) = pending.take() {
            self.settle(done, &mut entries);
        }

        let covers = match self.pages {
            true => self.ranges,
            false => Vec::new(),
        };
        ResolvedRecords {
            entries,
            covers,
            segments: self.segments,
            segment_min_lsn: self.segment_min_lsn,
            segment_max_lsn: self.segment_max_lsn,
            sealed: self.sealed,
            sealed_keys: self.sealed_keys,
            highest: self.highest,
        }
    }

    /// Land one key's winner the way the map join landed it
    ///
    /// A delete that wins installs nothing on a resident rebuild, since every version
    /// of the key went through the join and there is no record left to shadow. A paged
    /// rebuild has read no sealed row, so its delete goes in as a grave naming the
    /// segment its tombstone landed in, which stops the footer search.
    fn settle(
        &mut self,
        (key, record): (RecordKey, SeenRecord),
        entries: &mut HashMap<ColumnId, Vec<(KeyBytes, Entry)>>,
    ) {
        if record.is_tombstone || self.is_covered(&key, record.lsn) {
            if !record.is_tombstone {
                self.segments.entry(record.segment).or_default().dead += record.span();
            } else if self.pages {
                entries
                    .entry(key.column)
                    .or_default()
                    .push((key.key, Entry::grave_from(record.lsn, record.segment)));
            }
            return;
        }
        self.segments.entry(record.segment).or_default().live += record.span();
        let loc = Loc::new(record.segment, record.offset, record.len);
        entries
            .entry(key.column)
            .or_default()
            .push((key.key, Entry::new(loc, record.lsn)));
    }

    /// Whether a range tombstone drawn after this record covers its key
    fn is_covered(&self, key: &RecordKey, lsn: Lsn) -> bool {
        self.ranges.iter().any(|range| range.covers(key, lsn))
    }
}

/// Which of two runs' heads comes out first, by key, then sequence, then run
///
/// Key first, then sequence, so equal keys come out oldest first and the winner logic
/// keeps the last strict riser; the run breaks an exact tie in favour of the earliest
/// source. A drained run loses every match it plays, so it ends without bookkeeping.
fn beats(heads: &[Option<(RecordKey, SeenRecord)>], one: u32, two: u32) -> bool {
    match (&heads[one as usize], &heads[two as usize]) {
        (None, _) => false,
        (Some(_), None) => true,
        (Some((one_key, one_record)), Some((two_key, two_record))) => one_key
            .cmp(two_key)
            .then_with(|| one_record.lsn.cmp(&two_record.lsn))
            .then(one.cmp(&two))
            .is_lt(),
    }
}

/// No leaf, for a runner-up that has not been played for yet
const NO_LEAF: u32 = u32::MAX;

/// The runs joined in one tournament, each node holding the loser played there
///
/// Every run is already sorted and their count is known before the merge starts, so
/// replacing the record that just came out is one walk from its leaf to the root along
/// a fixed path. Whoever comes out next is either the run that just emitted or one of
/// the leaves that lost to it on the way up, so the best of those losers is named once
/// per walk and the next record costs one comparison against it. That runner-up is not
/// the root, since the second best is as likely to sit low on the champion's own path.
struct LoserTree {
    /// What is left of each run, drawn from as its leaf empties
    runs: Vec<std::vec::IntoIter<(RecordKey, SeenRecord)>>,

    /// The head of each run, one entry per leaf so the leaves past the last run are
    /// drained sentinels from the start
    heads: Vec<Option<(RecordKey, SeenRecord)>>,

    /// The leaf that lost the match played at each internal node, leaf `l` playing
    /// at node `(l + tree.len()) >> 1` and upwards with index zero unused
    tree: Vec<u32>,

    /// The leaf holding the lowest head, which is the next record out
    champion: u32,

    /// The best leaf the champion beat on its way up, which is the one to beat next
    runner_up: u32,
}

impl LoserTree {
    /// Seat every run at a leaf and play the tournament once
    fn new(runs: Vec<Vec<(RecordKey, SeenRecord)>>) -> LoserTree {
        // A power of two of leaves so a leaf's node is arithmetic, and at least two
        // so the root is a node even when a single run is joined against nothing.
        let leaves = runs.len().max(2).next_power_of_two();
        let mut runs: Vec<std::vec::IntoIter<(RecordKey, SeenRecord)>> =
            runs.into_iter().map(|run| run.into_iter()).collect();
        let mut heads: Vec<Option<(RecordKey, SeenRecord)>> =
            runs.iter_mut().map(|run| run.next()).collect();
        heads.resize_with(leaves, || None);

        // Bottom up, once: each node's winner goes up as that subtree's player and its
        // loser stays behind, which is the state every replay afterwards keeps.
        let mut tree = vec![0u32; leaves];
        let mut winners = vec![0u32; leaves * 2];
        for leaf in 0..leaves {
            winners[leaves + leaf] = leaf as u32;
        }
        for node in (1..leaves).rev() {
            let (left, right) = (winners[node * 2], winners[node * 2 + 1]);
            let (winner, loser) = match beats(&heads, right, left) {
                true => (right, left),
                false => (left, right),
            };
            winners[node] = winner;
            tree[node] = loser;
        }

        let champion = winners[1];
        let mut merge = LoserTree {
            runs,
            heads,
            tree,
            champion,
            runner_up: NO_LEAF,
        };
        // The winner is out of the tree, so replaying it changes nothing and names
        // the runner-up, which is the one thing the build above does not leave.
        merge.replay(champion);
        merge
    }

    /// The next record in key order, or nothing once every run has drained
    fn pop(&mut self) -> Option<(RecordKey, SeenRecord)> {
        let champion = self.champion as usize;
        let out = self.heads[champion].take()?;
        self.heads[champion] = self.runs.get_mut(champion).and_then(|run| run.next());
        // A run whose next record still beats the runner-up wins every match it would
        // replay, so the tree is already what a replay would leave.
        if !beats(&self.heads, self.champion, self.runner_up) {
            self.replay(self.champion);
        }
        Some(out)
    }

    /// Walk one leaf's new head to the root, leaving each match's loser behind
    fn replay(&mut self, mut candidate: u32) {
        let mut node = (candidate as usize + self.tree.len()) >> 1;
        while node >= 1 {
            if beats(&self.heads, self.tree[node], candidate) {
                std::mem::swap(&mut self.tree[node], &mut candidate);
            }
            node >>= 1;
        }
        self.champion = candidate;
        self.runner_up = self.best_loser(candidate);
    }

    /// The best leaf the champion beat on its way up
    ///
    /// A second walk rather than a best kept during the replay, since a replay ending
    /// in a swap brings the winner up out of the other subtree and the matches it won
    /// below that point were never walked.
    fn best_loser(&self, leaf: u32) -> u32 {
        let mut node = (leaf as usize + self.tree.len()) >> 1;
        let mut best = NO_LEAF;
        while node >= 1 {
            if best == NO_LEAF || beats(&self.heads, self.tree[node], best) {
                best = self.tree[node];
            }
            node >>= 1;
        }
        best
    }
}

fn note_segment_min(min_lsn: &mut HashMap<SegmentId, Lsn>, segment: SegmentId, lsn: Lsn) {
    let slot = min_lsn.entry(segment).or_insert(lsn);
    if lsn < *slot {
        *slot = lsn;
    }
}

pub(crate) fn read_footer(
    driver: &IoDriver,
    file: FileId,
    file_len: u64,
) -> Result<Option<SegmentFooter>> {
    let min_footer = FIXED_TAIL_LEN as u64;
    if file_len < min_footer {
        return Ok(None);
    }
    // An aligned write can land the footer with a block's worth of zeros after
    // it, so the trailer is read at the last byte that is not padding rather
    // than at the file end.
    let probe = file_len.min(TRAILER_PROBE_LEN);
    let padded = driver.pread(file, file_len - probe, probe)?;
    if (padded.len() as u64) < probe {
        return Ok(None);
    }
    let Some(last) = padded.iter().rposition(|byte| *byte != 0) else {
        return Ok(None);
    };
    let end = file_len - probe + last as u64 + 1;
    if end < min_footer {
        return Ok(None);
    }
    let trailer = driver.pread(file, end - TRAILER_LEN, TRAILER_LEN)?;
    if (trailer.len() as u64) < TRAILER_LEN {
        return Ok(None);
    }
    let footer_len = u64::from(read_u32_le(&trailer[0..4]));
    if footer_len < min_footer || footer_len > end {
        return Ok(None);
    }

    let footer_bytes = driver.pread(file, end - footer_len, footer_len)?;
    if (footer_bytes.len() as u64) < footer_len {
        return Ok(None);
    }
    match SegmentFooter::parse(&footer_bytes) {
        Ok(footer) => Ok(Some(footer)),
        Err(_) => Ok(None),
    }
}

/// Whether a record of this kind takes part in resolving keys
fn is_indexable(flags: Flags) -> bool {
    flags.is_data() || flags.is_tombstone() || flags.is_range_tombstone()
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::cmp::Reverse;
    use std::collections::BinaryHeap;
    use std::path::PathBuf;
    use std::sync::Arc;

    use crate::units::ByteCount;

    use crate::append::admission::InflightBudget;
    use crate::append::{Appender, BatchRecord, BatchWrite, Commit};
    use crate::config::{Preallocate, ReelConfig, SyncPolicy, DEFAULT_FD_CACHE};
    use crate::format::column::{Codec, ColumnSet, ColumnSpec, KeyWidth, MapShape};
    use crate::io::fault::FaultPlan;
    use crate::io::op::WriteBuf;
    use crate::io::sim_backend::{DurableImage, SimIo};
    use crate::reel::segment::{FdCache, IoDriver};
    use crate::reel::ReelShared;

    const REEL_DIR: &str = "/bulk/reel";
    const RECORDS: ColumnId = ColumnId(1);
    const META: ColumnId = ColumnId(2);

    const COLUMNS: ColumnSet = &[
        ColumnSpec {
            id: RECORDS,
            name: "records",
            key_width: KeyWidth::Fixed(34),
            shard_bytes: 2,
            inline_max: 0,
            row_carry: 0,
            purge_mark: None,
            codec: Codec::None,
            map_shape: MapShape::Tree,
        },
        ColumnSpec {
            id: META,
            name: "meta",
            key_width: KeyWidth::Fixed(32),
            shard_bytes: 1,
            inline_max: 0,
            row_carry: 0,
            purge_mark: None,
            codec: Codec::None,
            map_shape: MapShape::Tree,
        },
    ];

    fn config(sync: SyncPolicy) -> ReelConfig {
        ReelConfig {
            segment_bytes: ByteCount::mb(1),
            alloc_chunk: ByteCount::from_bytes(4096 * 4),
            preallocate: Preallocate::Chunk,
            sync,
            ..ReelConfig::default()
        }
    }

    fn shared(config: ReelConfig, sim: &SimIo) -> Arc<ReelShared> {
        let driver = Arc::new(IoDriver::new(Arc::new(sim.clone())));
        let budget = Arc::new(InflightBudget::default());
        let fd_cache = Arc::new(FdCache::new(DEFAULT_FD_CACHE as usize));
        Arc::new(ReelShared::new(
            PathBuf::from(REEL_DIR),
            driver,
            budget,
            fd_cache,
            config,
            COLUMNS,
            1,
        ))
    }

    fn key(byte: u8) -> RecordKey {
        RecordKey::from_bytes(RECORDS, &[byte; 34]).expect("key")
    }

    fn meta_key(byte: u8) -> RecordKey {
        RecordKey::from_bytes(META, &[byte; 32]).expect("key")
    }

    fn rebuild(sim: &SimIo) -> RebuiltReel {
        let driver = IoDriver::new(Arc::new(sim.clone()));
        rebuild_reel(&driver, &[PathBuf::from(REEL_DIR)], &[false], false).expect("rebuild")
    }

    fn count(rebuilt: &RebuiltReel) -> usize {
        rebuilt.entries.values().map(|rows| rows.len()).sum()
    }

    fn keys_of(rebuilt: &RebuiltReel, column: ColumnId) -> Vec<Vec<u8>> {
        rebuilt
            .entries
            .get(&column)
            .map(|rows| {
                rows.iter()
                    .map(|(key, _)| key.as_slice().to_vec())
                    .collect()
            })
            .unwrap_or_default()
    }

    // an overwrite-heavy source is held at its survivors, not at every version
    #[test]
    fn a_run_is_folded_to_its_survivors() {
        const KEYS: u8 = 8;
        const VERSIONS: u64 = 64;
        let mut resolver = Resolver::new(false);
        let mut run = Vec::new();
        for version in 0..VERSIONS {
            for byte in 0..KEYS {
                run.push((
                    key(byte),
                    SeenRecord {
                        lsn: Lsn(version * u64::from(KEYS) + u64::from(byte) + 1),
                        segment: SegmentId(1),
                        offset: 0,
                        len: 400,
                        key_width: 34,
                        is_tombstone: false,
                    },
                ));
            }
        }
        let versions = run.len();
        resolver.absorb_unsorted_run(run);

        let held: usize = resolver.runs.iter().map(Vec::len).sum();
        assert_eq!(held, KEYS as usize, "the run holds one version a key");
        assert!(held < versions, "which is under what the source handed over");

        let resolved = resolver.finish();
        let entries = resolved.entries.get(&RECORDS).expect("records");
        assert_eq!(entries.len(), KEYS as usize);
        let newest = (VERSIONS - 1) * u64::from(KEYS);
        for (_, entry) in entries {
            assert!(entry.lsn > Lsn(newest), "each key kept its newest version");
        }

        // Every version that lost is booked dead where it lay, folded or joined.
        let span = span_of(34, 400);
        let bytes = resolved.segments.get(&SegmentId(1)).expect("segment");
        assert_eq!(bytes.dead, (versions - KEYS as usize) as u64 * span);
        assert_eq!(bytes.live, KEYS as u64 * span);
    }

    // a sealed segment rebuilds its live records from the footer alone
    #[test]
    fn rebuilds_from_footer() {
        let sim = SimIo::new(FaultPlan::new(1));
        let shared = shared(config(SyncPolicy::Never), &sim);
        let appender = Appender::open(Arc::clone(&shared), 0, None).expect("open");
        appender
            .append_data(key(1), vec![0x11; 400], 0, Commit::PerRecord)
            .expect("put");
        appender
            .append_data(key(2), vec![0x22; 600], 0, Commit::PerRecord)
            .expect("put");
        appender.seal().expect("seal");

        let rebuilt = rebuild(&sim);

        assert_eq!(count(&rebuilt), 2);
        assert_eq!(rebuilt.highest_lsn, Lsn(2));
        assert_eq!(rebuilt.highest_segment, SegmentId(2));
        assert!(rebuilt.quarantined.is_empty());
    }

    // records of two columns rebuild into their own key sets
    #[test]
    fn rebuilds_columns_apart() {
        let sim = SimIo::new(FaultPlan::new(1));
        let shared = shared(config(SyncPolicy::Never), &sim);
        let appender = Appender::open(Arc::clone(&shared), 0, None).expect("open");
        appender
            .append_data(key(1), vec![0x11; 400], 0, Commit::PerRecord)
            .expect("put");
        appender
            .append_data(meta_key(1), vec![0x22; 600], 0, Commit::PerRecord)
            .expect("put");
        appender.seal().expect("seal");

        let rebuilt = rebuild(&sim);

        assert_eq!(keys_of(&rebuilt, RECORDS), vec![vec![1u8; 34]]);
        assert_eq!(keys_of(&rebuilt, META), vec![vec![1u8; 32]]);
    }

    // a newer overwrite wins the rebuild and the older copy is booked dead
    #[test]
    fn newest_lsn_wins() {
        let sim = SimIo::new(FaultPlan::new(1));
        let shared = shared(config(SyncPolicy::Never), &sim);
        let appender = Appender::open(Arc::clone(&shared), 0, None).expect("open");
        appender
            .append_data(key(1), vec![0x11; 400], 0, Commit::PerRecord)
            .expect("first");
        appender
            .append_data(key(1), vec![0x22; 900], 0, Commit::PerRecord)
            .expect("second");
        appender.seal().expect("seal");

        let rebuilt = rebuild(&sim);

        assert_eq!(count(&rebuilt), 1);
        let (_, entry) = rebuilt.entries[&RECORDS][0];
        assert_eq!(entry.lsn, Lsn(2));
        assert_eq!(entry.loc.len, 900);
    }

    // a tombstone that wins the rebuild leaves its key absent
    #[test]
    fn tombstone_wins_absent() {
        let sim = SimIo::new(FaultPlan::new(1));
        let shared = shared(config(SyncPolicy::Never), &sim);
        let appender = Appender::open(Arc::clone(&shared), 0, None).expect("open");
        appender
            .append_data(key(1), vec![0x11; 400], 0, Commit::PerRecord)
            .expect("put");
        appender
            .append_tombstone(key(1), Commit::PerRecord)
            .expect("delete");
        appender.seal().expect("seal");

        let rebuilt = rebuild(&sim);

        assert_eq!(count(&rebuilt), 0);
        assert_eq!(rebuilt.highest_lsn, Lsn(2));
    }

    // a range tombstone drops every older key it covers and spares the rest
    #[test]
    fn range_tombstone_replays() {
        let sim = SimIo::new(FaultPlan::new(1));
        let shared = shared(config(SyncPolicy::Never), &sim);
        let appender = Appender::open(Arc::clone(&shared), 0, None).expect("open");
        appender
            .append_data(key(1), vec![0x11; 100], 0, Commit::PerRecord)
            .expect("put");
        appender
            .append_data(key(3), vec![0x33; 100], 0, Commit::PerRecord)
            .expect("put");
        appender
            .append_data(meta_key(1), vec![0x44; 100], 0, Commit::PerRecord)
            .expect("put");
        appender
            .append_range_tombstone(key(0), Some(&[2u8; 34]), Commit::PerRecord)
            .expect("range delete");
        appender.seal().expect("seal");

        let rebuilt = rebuild(&sim);

        assert_eq!(keys_of(&rebuilt, RECORDS), vec![vec![3u8; 34]]);
        assert_eq!(keys_of(&rebuilt, META).len(), 1);
    }

    // a key written after a range tombstone survives the rebuild
    #[test]
    fn range_tombstone_spares_newer() {
        let sim = SimIo::new(FaultPlan::new(1));
        let shared = shared(config(SyncPolicy::Never), &sim);
        let appender = Appender::open(Arc::clone(&shared), 0, None).expect("open");
        appender
            .append_range_tombstone(key(0), None, Commit::PerRecord)
            .expect("range delete");
        appender
            .append_data(key(1), vec![0x11; 100], 0, Commit::PerRecord)
            .expect("put");
        appender.seal().expect("seal");

        let rebuilt = rebuild(&sim);

        assert_eq!(keys_of(&rebuilt, RECORDS), vec![vec![1u8; 34]]);
    }

    // an active tail rebuilds every record committed before the crash
    #[test]
    fn rebuilds_active_tail() {
        let sim = SimIo::new(FaultPlan::new(1));
        let shared = shared(config(SyncPolicy::EveryPut), &sim);
        let appender = Appender::open(Arc::clone(&shared), 0, None).expect("open");
        appender
            .append_data(key(1), vec![0x11; 400], 0, Commit::PerRecord)
            .expect("put");
        appender
            .append_data(key(2), vec![0x22; 600], 0, Commit::PerRecord)
            .expect("put");

        let rebuilt = rebuild(&sim);

        assert_eq!(count(&rebuilt), 2);
        assert_eq!(rebuilt.highest_segment, SegmentId(1));
    }

    // a whole batch in an unsealed tail rebuilds together
    #[test]
    fn whole_batch_rebuilds() {
        let sim = SimIo::new(FaultPlan::new(1));
        let shared = shared(config(SyncPolicy::EveryPut), &sim);
        let appender = Appender::open(Arc::clone(&shared), 0, None).expect("open");
        appender
            .append_batch(vec![
                BatchRecord {
                    key: key(1),
                    write: BatchWrite::Put(vec![0x11; 300], 0),
                },
                BatchRecord {
                    key: meta_key(2),
                    write: BatchWrite::Put(vec![0x22; 300], 0),
                },
            ])
            .expect("batch");

        let rebuilt = rebuild(&sim);

        assert_eq!(count(&rebuilt), 2);
    }

    // a batch whose closing mark never landed leaves nothing behind
    #[test]
    fn torn_batch_is_dropped() {
        let sim = SimIo::new(FaultPlan::new(1));
        let shared = shared(config(SyncPolicy::EveryPut), &sim);
        let appender = Appender::open(Arc::clone(&shared), 0, None).expect("open");
        appender
            .append_data(key(9), vec![0x99; 200], 0, Commit::PerRecord)
            .expect("put");
        let committed = appender
            .append_batch(vec![
                BatchRecord {
                    key: key(1),
                    write: BatchWrite::Put(vec![0x11; 300], 0),
                },
                BatchRecord {
                    key: key(2),
                    write: BatchWrite::Put(vec![0x22; 300], 0),
                },
            ])
            .expect("batch");
        corrupt_payload(&sim, u64::from(committed[1].loc.offset));

        let rebuilt = rebuild(&sim);

        assert_eq!(keys_of(&rebuilt, RECORDS), vec![vec![9u8; 34]]);
    }

    // a batch whose first record rots takes the rest of the run with it
    #[test]
    fn batch_with_first_record_torn_is_dropped() {
        let sim = SimIo::new(FaultPlan::new(1));
        let shared = shared(config(SyncPolicy::EveryPut), &sim);
        let appender = Appender::open(Arc::clone(&shared), 0, None).expect("open");
        appender
            .append_data(key(9), vec![0x99; 200], 0, Commit::PerRecord)
            .expect("put");
        let committed = appender
            .append_batch(vec![
                BatchRecord {
                    key: key(1),
                    write: BatchWrite::Put(vec![0x11; 300], 0),
                },
                BatchRecord {
                    key: key(2),
                    write: BatchWrite::Put(vec![0x22; 300], 0),
                },
            ])
            .expect("batch");
        corrupt_payload(&sim, u64::from(committed[0].loc.offset));

        let rebuilt = rebuild(&sim);

        assert_eq!(keys_of(&rebuilt, RECORDS), vec![vec![9u8; 34]]);
    }

    /// Write a batch of two behind one plain record, and say where its frame sits
    fn tail_with_a_batch(sim: &SimIo) -> u64 {
        let shared = shared(config(SyncPolicy::EveryPut), sim);
        let appender = Appender::open(Arc::clone(&shared), 0, None).expect("open");
        appender
            .append_data(key(9), vec![0x99; 200], 0, Commit::PerRecord)
            .expect("put");
        let committed = appender
            .append_batch(vec![
                BatchRecord {
                    key: key(1),
                    write: BatchWrite::Put(vec![0x11; 300], 0),
                },
                BatchRecord {
                    key: key(2),
                    write: BatchWrite::Put(vec![0x22; 300], 0),
                },
            ])
            .expect("batch");
        u64::from(committed[0].loc.offset) - BatchFrame::SPAN
    }

    // a batch whose frame rots is dropped whole, since nothing says where it ends
    #[test]
    fn a_torn_frame_drops_its_batch() {
        let sim = SimIo::new(FaultPlan::new(1));
        let frame_at = tail_with_a_batch(&sim);
        // The declaration itself, which the frame's own checksum covers.
        corrupt_at(&sim, frame_at + HEADER_LEN as u64, &[0xff; 4]);

        let rebuilt = rebuild(&sim);

        assert_eq!(keys_of(&rebuilt, RECORDS), vec![vec![9u8; 34]]);
    }

    // a batch whose records never landed is dropped, frame and all
    #[test]
    fn a_batch_the_write_never_reached_is_dropped() {
        let sim = SimIo::new(FaultPlan::new(1));
        let frame_at = tail_with_a_batch(&sim);
        // The shape a writev that stopped inside the batch leaves: the frame is there
        // and the run behind it is the reservation's own zeros.
        zero_from(&sim, frame_at + BatchFrame::SPAN, 4096);

        let rebuilt = rebuild(&sim);

        assert_eq!(keys_of(&rebuilt, RECORDS), vec![vec![9u8; 34]]);
    }

    // a batch missing its last record is dropped rather than half applied
    #[test]
    fn a_batch_cut_short_is_dropped() {
        let sim = SimIo::new(FaultPlan::new(1));
        let frame_at = tail_with_a_batch(&sim);
        let first = frame_at + BatchFrame::SPAN + HEADER_LEN as u64 + 34 + 300;
        zero_from(&sim, first, 4096);

        let rebuilt = rebuild(&sim);

        assert_eq!(keys_of(&rebuilt, RECORDS), vec![vec![9u8; 34]]);
    }

    // a record of a run whose frame is gone is not applied on its own
    #[test]
    fn a_member_without_its_frame_is_dropped() {
        let sim = SimIo::new(FaultPlan::new(1));
        let frame_at = tail_with_a_batch(&sim);
        // A pad in the frame's place, which is what a failed write leaves behind: the
        // records are all there and nothing vouches for them as a run.
        let pad = RecordHeader::fill(BatchFrame::SPAN as u32 - HEADER_LEN as u32);
        corrupt_at(&sim, frame_at, pad.pack().as_slice());

        let rebuilt = rebuild(&sim);

        assert_eq!(keys_of(&rebuilt, RECORDS), vec![vec![9u8; 34]]);
    }

    // a segment header naming another segment is quarantined, not indexed
    #[test]
    fn foreign_segment_quarantined() {
        let sim = SimIo::new(FaultPlan::new(1));
        let shared = shared(config(SyncPolicy::Never), &sim);
        let appender = Appender::open(Arc::clone(&shared), 0, None).expect("open");
        appender
            .append_data(key(1), vec![0x11; 400], 0, Commit::PerRecord)
            .expect("put");
        appender.seal().expect("seal");
        write_foreign_segment(&sim);

        let rebuilt = rebuild(&sim);

        assert_eq!(count(&rebuilt), 1);
        assert_eq!(rebuilt.quarantined.len(), 1);
        assert!(sim
            .durable_bytes(Path::new(REEL_DIR).join("000099.reel").as_path())
            .is_some());
    }

    /// Cut a sealed segment back to its record region, the shape a failed seal leaves
    fn strip_footer(image: &mut DurableImage, name: &str) {
        for (path, bytes) in image.iter_mut() {
            if path.file_name().map(|found| found == name).unwrap_or(false) {
                let len = bytes.len();
                let footer_len =
                    u32::from_le_bytes(bytes[len - 8..len - 4].try_into().expect("trailer"))
                        as usize;
                bytes.truncate(len - footer_len);
            }
        }
    }

    // a stale footerless segment cannot shadow what sealed after it on a paged rebuild
    #[test]
    fn a_stale_footerless_segment_does_not_shadow_paged() {
        let sim = SimIo::new(FaultPlan::new(1));
        let shared = shared(config(SyncPolicy::Never), &sim);
        let appender = Appender::open(Arc::clone(&shared), 0, None).expect("open");
        appender
            .append_data(key(1), vec![0x11; 300], 0, Commit::PerRecord)
            .expect("old version");
        appender
            .append_data(key(2), vec![0x22; 300], 0, Commit::PerRecord)
            .expect("sole version");
        appender
            .append_tombstone(key(3), Commit::PerRecord)
            .expect("old delete");
        appender.seal().expect("seal one");
        appender
            .append_data(key(1), vec![0x33; 300], 0, Commit::PerRecord)
            .expect("new version");
        appender
            .append_data(key(3), vec![0x44; 300], 0, Commit::PerRecord)
            .expect("rewritten");
        appender.seal().expect("seal two");

        let mut image = sim.durable_image();
        strip_footer(&mut image, "000001.reel");
        let torn = SimIo::from_image(image);
        let driver = IoDriver::new(Arc::new(torn));
        let rebuilt =
            rebuild_reel(&driver, &[PathBuf::from(REEL_DIR)], &[false], true).expect("rebuild");

        let rows = rebuilt.entries.get(&RECORDS).cloned().unwrap_or_default();
        assert!(
            !rows.iter().any(|(key, _)| key.as_slice() == [1u8; 34]),
            "the walked old version shadows the sealed rewrite"
        );
        assert!(
            !rows.iter().any(|(key, _)| key.as_slice() == [3u8; 34]),
            "the walked tombstone left a grave over the sealed rewrite"
        );
        assert!(
            rows.iter()
                .any(|(key, entry)| key.as_slice() == [2u8; 34] && !entry.is_grave()),
            "a key with no newer sealed version lost its only record to the prune"
        );
    }

    // a failed write's range is stamped, so records above it survive a reopen; the
    // fault position is searched for since the op count moves with the write path
    #[test]
    fn a_failed_write_does_not_strand_later_records() {
        let mut produced = 0;
        for at in 2..32u64 {
            let plan = FaultPlan::new(1).with_fault(at, crate::io::fault::FaultKind::EnospcAppend);
            let sim = SimIo::new(plan);
            let shared = shared(config(SyncPolicy::Never), &sim);
            let Ok(appender) = Appender::open(Arc::clone(&shared), 0, None) else {
                continue;
            };
            let first = appender.append_data(key(1), vec![0x11; 300], 0, Commit::PerRecord);
            let middle = appender.append_data(key(2), vec![0x22; 300], 0, Commit::PerRecord);
            let last = appender.append_data(key(1), vec![0x33; 300], 0, Commit::PerRecord);
            let (Ok(_), Err(_), Ok(newest)) = (first, middle, last) else {
                continue;
            };
            produced += 1;

            let rebuilt = rebuild(&sim);
            let rows = rebuilt.entries.get(&RECORDS).cloned().unwrap_or_default();
            let found = rows
                .iter()
                .find(|(key, _)| key.as_slice() == [1u8; 34])
                .unwrap_or_else(|| panic!("the record above the failed range went missing"));
            assert_eq!(
                found.1.lsn, newest.lsn,
                "a reopen resolved a version from below the failed range"
            );
            assert!(
                !rows.iter().any(|(key, _)| key.as_slice() == [2u8; 34]),
                "the failed write itself came back"
            );
        }
        assert!(
            produced > 0,
            "no fault position produced the failed-middle shape"
        );
    }

    // a torn record in an unsealed tail drops only that record
    #[test]
    fn torn_tail_drops_one_record() {
        let sim = SimIo::new(FaultPlan::new(1));
        let shared = shared(config(SyncPolicy::Never), &sim);
        let appender = Appender::open(Arc::clone(&shared), 0, None).expect("open");
        appender
            .append_data(key(1), vec![0x11; 400], 0, Commit::PerRecord)
            .expect("kept");
        let committed = appender
            .append_data(key(2), vec![0x22; 400], 0, Commit::PerRecord)
            .expect("torn");
        corrupt_payload(&sim, u64::from(committed.loc.offset));

        let rebuilt = rebuild(&sim);

        assert_eq!(keys_of(&rebuilt, RECORDS), vec![vec![1u8; 34]]);
    }

    fn write_foreign_segment(sim: &SimIo) {
        let driver = IoDriver::new(Arc::new(sim.clone()));
        let path = Path::new(REEL_DIR).join("000099.reel");
        let file = open_created(&driver, &path);
        let payload = SegmentHeader::new(SegmentId(7)).pack().to_vec();
        let header = RecordHeader::segment_header(&payload);
        let mut bytes = header.pack().as_slice().to_vec();
        bytes.extend_from_slice(&payload);
        write_all(&driver, file, &bytes);
    }

    fn corrupt_payload(sim: &SimIo, record_offset: u64) {
        let driver = IoDriver::new(Arc::new(sim.clone()));
        let path = Path::new(REEL_DIR).join("000001.reel");
        let file = open_created(&driver, &path);
        write_at(
            &driver,
            file,
            record_offset + HEADER_LEN as u64 + 34,
            &[0xff; 8],
        );
    }

    /// Overwrite the tail at an offset, the way rot or a stray write would
    fn corrupt_at(sim: &SimIo, offset: u64, bytes: &[u8]) {
        let driver = IoDriver::new(Arc::new(sim.clone()));
        let path = Path::new(REEL_DIR).join("000001.reel");
        let file = open_created(&driver, &path);
        write_at(&driver, file, offset, bytes);
    }

    /// Put the tail back to the zeros a reservation reads as from an offset
    fn zero_from(sim: &SimIo, offset: u64, len: usize) {
        corrupt_at(sim, offset, &vec![0u8; len]);
    }

    // The directory is synced behind a creation the way a real one is, so the file
    // is on the volume rather than only in a cache a crash would drop.
    fn open_created(driver: &IoDriver, path: &Path) -> FileId {
        let file = driver.open(path, true).expect("open");
        driver
            .sync_dir(path.parent().expect("parent"))
            .expect("sync dir");
        file
    }

    fn write_all(driver: &IoDriver, file: FileId, bytes: &[u8]) {
        write_at(driver, file, 0, bytes);
    }

    fn write_at(driver: &IoDriver, file: FileId, offset: u64, bytes: &[u8]) {
        driver
            .writev(file, offset, vec![WriteBuf::owned(bytes.to_vec())])
            .expect("write");
    }

    fn segment_len(sim: &SimIo, name: &str) -> u64 {
        let driver = IoDriver::new(Arc::new(sim.clone()));
        let entries = driver.list(Path::new(REEL_DIR)).expect("list");
        entries
            .iter()
            .find(|entry| entry.name == name)
            .map(|entry| entry.len)
            .expect("segment listed")
    }

    fn truncate_segment(image: &mut DurableImage, name: &str, cut: u64) {
        for (path, bytes) in image.iter_mut() {
            if path.file_name().map(|found| found == name).unwrap_or(false) {
                let keep = bytes.len().saturating_sub(cut as usize);
                bytes.truncate(keep);
            }
        }
    }

    /// Bytes a sealed footer of this many record rows takes
    fn footer_len_for(rows: u32) -> u64 {
        use crate::format::footer::{FooterEntry, SegmentFooter};

        let entries = (0..rows)
            .map(|at| FooterEntry::new(key(at as u8), Lsn(1), at, 400, Flags::DATA))
            .collect();
        SegmentFooter::build(entries).pack(0).expect("pack").len() as u64
    }

    fn sealed_pair(sim: &SimIo) {
        let shared = shared(config(SyncPolicy::Never), sim);
        let appender = Appender::open(Arc::clone(&shared), 0, None).expect("open");
        appender
            .append_data(key(1), vec![0x11; 400], 0, Commit::PerRecord)
            .expect("put");
        appender
            .append_data(key(2), vec![0x22; 600], 0, Commit::PerRecord)
            .expect("put");
        appender.seal().expect("seal");
    }

    // a footer with a corrupt magic falls back to a record scan
    #[test]
    fn torn_footer_scans() {
        let sim = SimIo::new(FaultPlan::new(1));
        sealed_pair(&sim);
        let length = segment_len(&sim, "000001.reel");
        let driver = IoDriver::new(Arc::new(sim.clone()));
        let file = open_created(&driver, &Path::new(REEL_DIR).join("000001.reel"));

        write_at(&driver, file, length - 1, &[0xff]);
        let rebuilt = rebuild(&sim);

        let found = keys_of(&rebuilt, RECORDS);
        assert!(found.contains(&vec![1u8; 34]));
        assert!(found.contains(&vec![2u8; 34]));
    }

    // a segment sealed only partially, its footer missing, is record scanned
    #[test]
    fn partial_footer_scans() {
        let sim = SimIo::new(FaultPlan::new(1));
        sealed_pair(&sim);
        let mut image = sim.durable_image();
        let cut = footer_len_for(2);

        truncate_segment(&mut image, "000001.reel", cut);
        let torn = SimIo::from_image(image);
        let rebuilt = rebuild(&torn);

        let found = keys_of(&rebuilt, RECORDS);
        assert!(found.contains(&vec![1u8; 34]));
        assert!(found.contains(&vec![2u8; 34]));
    }

    // the sequence counter reinitializes above the highest number seen
    #[test]
    fn lsn_counter_reinitializes() {
        let sim = SimIo::new(FaultPlan::new(1));
        sealed_pair(&sim);

        let rebuilt = rebuild(&sim);

        assert_eq!(rebuilt.highest_lsn, Lsn(2));
    }

    /// What one merged record is compared by, since a seen record has no derives
    type Row = (ColumnId, Vec<u8>, u64, u32, u32, u32, bool);

    fn row(key: &RecordKey, record: &SeenRecord) -> Row {
        (
            key.column,
            key.key.as_slice().to_vec(),
            record.lsn.as_u64(),
            record.segment.as_u32(),
            record.offset,
            record.len,
            record.is_tombstone,
        )
    }

    /// The merge the loser tree replaced, kept as the answer it has to agree with
    ///
    /// A binary heap over one head per run in the same order, so a difference between
    /// the two sequences is the tree's.
    fn heap_merge(runs: Vec<Vec<(RecordKey, SeenRecord)>>) -> Vec<Row> {
        struct Head {
            key: RecordKey,
            record: SeenRecord,
            run: usize,
        }

        impl PartialEq for Head {
            fn eq(&self, other: &Head) -> bool {
                self.cmp(other) == std::cmp::Ordering::Equal
            }
        }

        impl Eq for Head {}

        impl PartialOrd for Head {
            fn partial_cmp(&self, other: &Head) -> Option<std::cmp::Ordering> {
                Some(self.cmp(other))
            }
        }

        impl Ord for Head {
            fn cmp(&self, other: &Head) -> std::cmp::Ordering {
                self.key
                    .cmp(&other.key)
                    .then_with(|| self.record.lsn.cmp(&other.record.lsn))
                    .then_with(|| self.run.cmp(&other.run))
            }
        }

        let mut iters: Vec<std::vec::IntoIter<(RecordKey, SeenRecord)>> =
            runs.into_iter().map(|run| run.into_iter()).collect();
        let mut heap: BinaryHeap<Reverse<Head>> = BinaryHeap::with_capacity(iters.len());
        for (run, iter) in iters.iter_mut().enumerate() {
            if let Some((key, record)) = iter.next() {
                heap.push(Reverse(Head { key, record, run }));
            }
        }

        let mut out = Vec::new();
        while let Some(Reverse(head)) = heap.pop() {
            if let Some((key, record)) = iters[head.run].next() {
                heap.push(Reverse(Head {
                    key,
                    record,
                    run: head.run,
                }));
            }
            out.push(row(&head.key, &head.record));
        }
        out
    }

    /// A key numbered within a column, big endian so the number is the key order
    fn numbered(column: ColumnId, width: usize, number: u64) -> RecordKey {
        let mut bytes = vec![0u8; width];
        bytes[..8].copy_from_slice(&number.to_be_bytes());
        RecordKey::from_bytes(column, &bytes).expect("key")
    }

    fn seen(lsn: u64, segment: u32, width: u16) -> SeenRecord {
        SeenRecord {
            lsn: Lsn(lsn),
            segment: SegmentId(segment),
            offset: (lsn % 4096) as u32 * 64,
            len: 100,
            key_width: width,
            is_tombstone: lsn.is_multiple_of(17),
        }
    }

    fn draw(state: &mut u64) -> u64 {
        *state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        *state >> 33
    }

    /// Run sets shaped like the ones a rebuild actually joins
    #[derive(Clone, Copy, Debug)]
    enum Shape {
        /// Runs holding disjoint ascending ranges, which is what the segments of an
        /// ordered column are and where the runner-up shortcut earns its keep
        Disjoint { runs: usize, each: usize },

        /// One run and nothing else, which the shortcut turns into a drain
        Alone { records: usize },

        /// One long run against many tiny ones and an empty one, which is a walked
        /// tail beside the segments it was written over
        Skewed { runs: usize },

        /// Every run drawing from a small key space, so the same key is in many runs
        Overlapping {
            runs: usize,
            each: usize,
            space: u64,
        },

        /// The same key at the same sequence number in several runs, the only case
        /// where a tie falling to the earliest source shows
        Tied { runs: usize, each: usize },

        /// Two columns in every run, which is what one footer's partitions are
        Columns { runs: usize, each: usize },
    }

    /// Build one run set, deterministic from its seed
    ///
    /// Built twice per case rather than cloned, since the two merges consume theirs.
    fn run_set(shape: Shape, seed: u64) -> Vec<Vec<(RecordKey, SeenRecord)>> {
        let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
        let mut runs: Vec<Vec<(RecordKey, SeenRecord)>> = Vec::new();
        match shape {
            Shape::Disjoint { runs: count, each } => {
                for run in 0..count {
                    let base = (run * each) as u64;
                    runs.push(
                        (0..each)
                            .map(|at| {
                                let number = base + at as u64;
                                (
                                    numbered(RECORDS, 34, number),
                                    seen(number + 1, run as u32 + 1, 34),
                                )
                            })
                            .collect(),
                    );
                }
            }
            Shape::Alone { records } => {
                runs.push(
                    (0..records)
                        .map(|at| (numbered(RECORDS, 34, at as u64), seen(at as u64 + 1, 1, 34)))
                        .collect(),
                );
            }
            Shape::Skewed { runs: count } => {
                for run in 0..count {
                    let each = match run {
                        0 => 4096,
                        _ => draw(&mut state) as usize % 4,
                    };
                    runs.push(
                        (0..each)
                            .map(|at| {
                                let number = match run {
                                    0 => at as u64,
                                    _ => draw(&mut state) % 4096,
                                };
                                (
                                    numbered(RECORDS, 34, number),
                                    seen(draw(&mut state) % 8192 + 1, run as u32 + 1, 34),
                                )
                            })
                            .collect(),
                    );
                }
                // A segment whose rows were all range tombstones lists nothing here,
                // so a leaf can be drained before the tournament starts.
                runs.push(Vec::new());
            }
            Shape::Overlapping {
                runs: count,
                each,
                space,
            } => {
                let mut lsn = 1u64;
                for run in 0..count {
                    runs.push(
                        (0..each)
                            .map(|_| {
                                lsn += 1;
                                (
                                    numbered(RECORDS, 34, draw(&mut state) % space),
                                    seen(lsn, run as u32 + 1, 34),
                                )
                            })
                            .collect(),
                    );
                }
            }
            Shape::Tied { runs: count, each } => {
                for run in 0..count {
                    runs.push(
                        (0..each)
                            .map(|at| {
                                // Same key, same sequence number, different segment,
                                // which is one record listed by two sources.
                                (
                                    numbered(RECORDS, 34, at as u64),
                                    seen(at as u64 + 1, run as u32 + 1, 34),
                                )
                            })
                            .collect(),
                    );
                }
            }
            Shape::Columns { runs: count, each } => {
                for run in 0..count {
                    let mut rows = Vec::new();
                    for at in 0..each {
                        let number = draw(&mut state) % 512;
                        rows.push((
                            numbered(RECORDS, 34, number),
                            seen(at as u64 + 1, run as u32 + 1, 34),
                        ));
                        rows.push((
                            numbered(META, 32, number),
                            seen(at as u64 + 1, run as u32 + 1, 32),
                        ));
                    }
                    runs.push(rows);
                }
            }
        }

        // Every source hands the join a run already in key order, so the generator
        // owes the same rather than relying on how it happened to draw.
        for run in runs.iter_mut() {
            run.sort_by(|one, two| one.0.cmp(&two.0).then(one.1.lsn.cmp(&two.1.lsn)));
        }
        runs
    }

    // the loser tree emits exactly what the heap it replaced emitted
    #[test]
    fn the_tree_merges_what_the_heap_merged() {
        let shapes = [
            Shape::Disjoint { runs: 64, each: 32 },
            Shape::Disjoint {
                runs: 1000,
                each: 3,
            },
            Shape::Alone { records: 500 },
            Shape::Skewed { runs: 33 },
            Shape::Overlapping {
                runs: 16,
                each: 64,
                space: 40,
            },
            Shape::Tied { runs: 7, each: 20 },
            Shape::Columns { runs: 12, each: 25 },
        ];

        for shape in shapes {
            for seed in 1..=4u64 {
                let wanted = heap_merge(run_set(shape, seed));
                let mut tree = LoserTree::new(run_set(shape, seed));
                let mut found = Vec::new();
                while let Some((key, record)) = tree.pop() {
                    found.push(row(&key, &record));
                }

                assert!(
                    !wanted.is_empty(),
                    "{shape:?} at seed {seed} generated nothing to merge"
                );
                assert_eq!(found, wanted, "{shape:?} at seed {seed} merged differently");

                if let Shape::Tied { .. } = shape {
                    let ties = wanted
                        .windows(2)
                        .filter(|pair| pair[0].1 == pair[1].1 && pair[0].2 == pair[1].2)
                        .count();
                    assert!(
                        ties > 0,
                        "the tied shape left no tie for the rule to decide"
                    );
                }
            }
        }
    }
}
