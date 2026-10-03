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
use std::sync::{Condvar, Mutex};

use crate::config::ThreadBudget;
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
use crate::index::column::{KeyMove, Landed};
use crate::index::counters::SegmentBytes;
use crate::index::entry::{span_of, Entry};
use crate::index::map::ReelIndex;
use crate::index::persisted::{trusted, PersistedReader, PersistedSegment};
use crate::index::sealed_keys::SealedKeys;
use crate::io::op::FileId;
use crate::io::ServingBackend;
use crate::reel::segment::{IoDriver, SegmentReader};
use crate::reel::segment_number;
use crate::sync::lock;

/// Bytes at the very end of a sealed segment holding its footer length and magic
const TRAILER_LEN: u64 = 8;

/// Bytes of the file end searched for the trailer past any aligned-write zeros
const TRAILER_PROBE_LEN: u64 = 4096;

/// Threads a rebuild opens segment files on, however wide the machine is
const MAX_READERS: usize = 8;

/// Segment files each reader may read ahead of the join
const READ_AHEAD: usize = 4;

/// Records one column batch takes into the index at a time
const BATCH: usize = 4096;

/// Segments a resident rebuild holds before it feeds their rows in key order
const FEED_WINDOW: usize = MAX_READERS * READ_AHEAD;

/// What a reel rebuild hands back beside the index it filled
pub struct RebuiltReel {
    /// Highest sequence number seen, for reinitializing the counter
    pub highest_lsn: Lsn,

    /// Highest segment number present, for continuing the numbering
    pub highest_segment: SegmentId,

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

/// Rebuild the reel's index by reading every segment file in its directory into it
///
/// A paging volume sweeps its sealed segments rather than resolving them, so the peak
/// is one footer instead of one key set. Only the tails are resolved into memory.
pub fn rebuild_reel(
    driver: &IoDriver,
    roots: &[PathBuf],
    dead: &[bool],
    pages: bool,
    index: &ReelIndex,
) -> Result<RebuiltReel> {
    rebuild_from_persisted(driver, roots, dead, pages, None, index)
}

/// The same rebuild, offered an index a previous cue wrote down
///
/// The file speaks only for the segments still standing at the length it recorded,
/// and those are the only ones this skips reading. Everything else is read as it
/// would be with no file at all, and its rows go through the same sequence number
/// guard, so a stale file can never be the reason a version is missed.
///
/// A paging volume takes no offer, since its sealed keys stay in their footers.
pub fn rebuild_from_persisted(
    driver: &IoDriver,
    roots: &[PathBuf],
    dead: &[bool],
    pages: bool,
    persisted: Option<PersistedReader>,
    index: &ReelIndex,
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

    let standing = match &persisted {
        Some(reader) => {
            let present: HashMap<SegmentId, u64> = files
                .iter()
                .map(|(number, _, len, _)| (SegmentId(*number), *len))
                .collect();
            trusted(&reader.index, &present)
        }
        None => BTreeSet::new(),
    };

    let mut resolver = Resolver::new(index, pages);
    let mut quarantined = Vec::new();
    let mut consumed = HashMap::new();
    let mut walked = Vec::new();
    let mut resumable: Vec<ResumableTail> = Vec::new();
    let mut sealed_files = Vec::new();
    let mut placements = Vec::new();
    let mut highest_number = 0u32;
    let mut jobs: Vec<(SegmentId, PathBuf, u64)> = Vec::new();
    for (number, path, len, root) in files {
        highest_number = highest_number.max(number);
        let segment = SegmentId(number);
        if root != 0 {
            placements.push((segment, root));
        }
        if standing.contains(&segment) {
            // Sealed and vouched for, so the file is read to its end and the rows
            // the persisted index holds stand in for walking it.
            consumed.insert(segment, len);
            continue;
        }
        jobs.push((segment, path, len));
    }
    let mut held: Vec<Held> = Vec::new();
    read_segments(driver, &jobs, |at, parts| {
        let (segment, path, len) = &jobs[at];
        // A resident volume feeds a window of segments in key order, so each shard's
        // tree fills a leaf at a time.
        let loaded = match pages {
            true => absorb_segment(*segment, parts, pages, &mut resolver)?,
            false => hold_segment(*segment, parts, &mut held),
        };
        match loaded {
            Loaded::Sealed => {
                consumed.insert(*segment, *len);
                sealed_files.push((*segment, path.clone(), *len));
            }
            Loaded::Walked(offset, is_at_fill, entries) => {
                consumed.insert(*segment, offset);
                walked.push((path.clone(), *len));
                // Resumable means the walk ran out of written bytes. A walk that
                // stopped on something else has bytes ahead of it that an appender
                // must not write behind.
                if is_at_fill {
                    resumable.push(ResumableTail {
                        segment: *segment,
                        path: path.clone(),
                        end: offset,
                        entries,
                    });
                }
            }
            Loaded::Foreign => quarantined.push(path.clone()),
        }
        if held.len() >= FEED_WINDOW {
            feed_held(&mut held, &mut resolver, &mut resumable)?;
        }
        Ok(())
    })?;
    feed_held(&mut held, &mut resolver, &mut resumable)?;
    // A file that goes bad partway through its rows starts the rebuild over without it.
    if let Some(reader) = persisted {
        if let Err(error) = adopt(driver, reader, &standing, &mut resolver) {
            tracing::warn!(
                "the persisted index went bad partway through its rows, so this open \
                 sweeps the footers: {error}",
            );
            return rebuild_from_persisted(driver, roots, dead, pages, None, index);
        }
    }

    resolver.flush();
    if pages {
        prune_walked_shadowed(driver, &sealed_files, &resolver.sealed, index)?;
    }
    let highest_lsn = resolver.finish()?;
    resumable.sort_by_key(|tail| tail.segment.as_u32());
    Ok(RebuiltReel {
        highest_lsn,
        highest_segment: SegmentId(highest_number),
        placements,
        quarantined,
        consumed,
        walked,
        resumable,
    })
}

/// Fold a persisted index's stamps and rows in after the segments, so a tie falls to the segment
fn adopt(
    driver: &IoDriver,
    mut reader: PersistedReader,
    standing: &BTreeSet<SegmentId>,
    resolver: &mut Resolver<'_>,
) -> Result<()> {
    for stamp in &reader.index.segments {
        if standing.contains(&stamp.segment) {
            resolver.adopt_segment(stamp);
        }
    }
    resolver.see(reader.index.at);
    let read = (|| -> Result<()> {
        while reader.advance(driver)? {
            let loc = reader.loc();
            if !standing.contains(&loc.segment) {
                continue;
            }
            let key = RecordKey::from_bytes(reader.column(), reader.key())?;
            resolver.put(key.column, key.as_slice(), loc, reader.lsn(), false);
        }
        Ok(())
    })();
    reader.close(driver)?;
    read
}

/// What reading one segment during a rebuild turned out to be
enum Loaded {
    /// A sealed segment, read from its footer, with nothing left to follow
    Sealed,

    /// An unsealed tail, walked to an offset, whether it may be taken up again, and
    /// its rows packed as its footer holds them
    Walked(u64, bool, SegmentFooter),

    /// A file that is not a segment of this reel
    Foreign,
}

/// An unsealed tail an appender can pick up where it stopped
///
/// The end is the walked offset; the footer is rebuilt from the walk and carries
/// nothing inline, so a read through one of its rows goes to the record.
pub struct ResumableTail {
    pub segment: SegmentId,
    pub path: PathBuf,
    pub end: u64,
    pub entries: SegmentFooter,
}

/// One segment file read off the medium, before any of it is joined
///
/// Everything the join needs is in here, so absorbing a segment touches no
/// descriptor and the reads can run wherever there is a thread for them.
enum SegmentParts {
    /// A sealed segment's footer, and the range ends its rows do not carry
    Sealed(SegmentFooter, Vec<Option<KeyBytes>>),

    /// An unsealed tail's walk
    Walked(WalkedTail),

    /// A file that is not a segment of this reel
    Foreign,
}

/// Read every job's segment file, handing each to the join in job order
///
/// The reads are independent and the join is not: an exact tie between two runs falls
/// to the earliest source. So the files are read across threads while the join takes
/// them in order, and a reader runs no further ahead than the window, which holds the
/// peak at a few segments' parts rather than the volume's.
fn read_segments(
    driver: &IoDriver,
    jobs: &[(SegmentId, PathBuf, u64)],
    mut join: impl FnMut(usize, SegmentParts) -> Result<()>,
) -> Result<()> {
    let readers = match reads_on_its_caller(driver) {
        true => ThreadBudget::Auto
            .resolve()
            .clamp(1, MAX_READERS)
            .min(jobs.len()),
        false => 1,
    };
    if readers <= 1 {
        for (at, (segment, path, len)) in jobs.iter().enumerate() {
            join(at, read_segment(driver, path, *segment, *len)?)?;
        }
        return Ok(());
    }

    let queue = Mutex::new(ReadQueue {
        next: 0,
        done: HashMap::new(),
        taken: 0,
        stop: false,
    });
    let moved = Condvar::new();
    let window = readers * READ_AHEAD;
    let mut outcome = Ok(());
    std::thread::scope(|scope| {
        for _ in 0..readers {
            scope.spawn(|| read_claimed(driver, jobs, &queue, &moved, window));
        }
        for at in 0..jobs.len() {
            let parts = {
                let mut held = lock(&queue);
                let parts = loop {
                    match held.done.remove(&at) {
                        Some(parts) => break parts,
                        None => {
                            held = moved.wait(held).unwrap_or_else(|bad| bad.into_inner());
                        }
                    }
                };
                held.taken = at + 1;
                moved.notify_all();
                parts
            };
            outcome = parts.and_then(|parts| join(at, parts));
            if outcome.is_err() {
                lock(&queue).stop = true;
                moved.notify_all();
                break;
            }
        }
    });
    outcome
}

/// Whether a reader thread on this backend is another read in flight
///
/// A synchronous backend runs its syscall on whichever thread submitted it, so a second
/// thread is a second seek the drive can be working on. A ring has one queue and one
/// drain, and the simulator answers under one lock in submit order, so on both a
/// fan-out buys contention rather than depth.
fn reads_on_its_caller(driver: &IoDriver) -> bool {
    matches!(
        driver.serving(),
        ServingBackend::Posix | ServingBackend::PosixDirect
    )
}

/// What the readers have read and how far the join has got through it
struct ReadQueue {
    /// The next job a reader claims, since the files are read in order
    next: usize,

    /// Parts read and not yet joined, by job
    done: HashMap<usize, Result<SegmentParts>>,

    /// Jobs the join has taken, which is what the read-ahead window is measured from
    taken: usize,

    /// Set where the join gave up, so the readers stop with it
    stop: bool,
}

/// Claim jobs and read them until the list runs out or the join stops
fn read_claimed(
    driver: &IoDriver,
    jobs: &[(SegmentId, PathBuf, u64)],
    queue: &Mutex<ReadQueue>,
    moved: &Condvar,
    window: usize,
) {
    loop {
        let at = {
            let mut held = lock(queue);
            loop {
                if held.stop || held.next >= jobs.len() {
                    return;
                }
                if held.next < held.taken + window {
                    break;
                }
                held = moved.wait(held).unwrap_or_else(|bad| bad.into_inner());
            }
            let at = held.next;
            held.next += 1;
            at
        };
        let (segment, path, len) = &jobs[at];
        let parts = read_segment(driver, path, *segment, *len);
        lock(queue).done.insert(at, parts);
        moved.notify_all();
    }
}

/// Read one segment file, releasing its descriptor either way
///
/// A rebuild opens every segment file, so a descriptor left behind here is one per
/// segment on every open of the volume.
fn read_segment(
    driver: &IoDriver,
    path: &Path,
    segment: SegmentId,
    file_len: u64,
) -> Result<SegmentParts> {
    let file = driver.open(path, false)?;
    let read = read_parts(driver, file, segment, file_len);
    driver.close(file)?;
    read
}

fn read_parts(
    driver: &IoDriver,
    file: FileId,
    segment: SegmentId,
    file_len: u64,
) -> Result<SegmentParts> {
    if !belongs_here(driver, file, segment)? {
        return Ok(SegmentParts::Foreign);
    }

    match read_footer(driver, file, file_len)? {
        Some(footer) => {
            let ends = read_range_ends(driver, file, &footer)?;
            Ok(SegmentParts::Sealed(footer, ends))
        }
        None => {
            let mut reader = SegmentReader::new(driver, file, file_len);
            Ok(SegmentParts::Walked(walk_tail(&mut reader, segment, file_len)?))
        }
    }
}

/// Fold one segment's parts into the index
fn absorb_segment(
    segment: SegmentId,
    parts: SegmentParts,
    pages: bool,
    resolver: &mut Resolver<'_>,
) -> Result<Loaded> {
    match parts {
        SegmentParts::Foreign => Ok(Loaded::Foreign),
        SegmentParts::Sealed(footer, ends) => {
            let mut ends = ends.into_iter();
            match pages {
                true => sweep_footer(segment, &footer, &mut ends, resolver)?,
                false => collect_partitions(segment, &footer.partitions, &mut ends, resolver)?,
            }
            Ok(Loaded::Sealed)
        }
        SegmentParts::Walked(tail) => {
            let mut ends = tail.ends.into_iter();
            collect_partitions(segment, &tail.footer.partitions, &mut ends, resolver)?;
            Ok(Loaded::Walked(tail.next_offset, tail.is_at_fill, tail.footer))
        }
    }
}

/// One segment's rows a resident rebuild holds until its window is fed
struct Held {
    segment: SegmentId,
    footer: SegmentFooter,

    /// Each range tombstone's end, partition by partition in row order
    ends: Vec<Option<KeyBytes>>,

    /// Whether the rows sit in key order, which a sealed footer's do and a walk's do not
    is_sorted: bool,
}

/// Keep a resident segment's rows for the key-ordered feed
///
/// A walked tail's footer stays here until the feed and then goes to the tail resuming
/// it, so what the walk hands back for now is an empty one.
fn hold_segment(segment: SegmentId, parts: SegmentParts, held: &mut Vec<Held>) -> Loaded {
    match parts {
        SegmentParts::Foreign => Loaded::Foreign,
        SegmentParts::Sealed(footer, ends) => {
            held.push(Held { segment, footer, ends, is_sorted: true });
            Loaded::Sealed
        }
        SegmentParts::Walked(tail) => {
            held.push(Held { segment, footer: tail.footer, ends: tail.ends, is_sorted: false });
            Loaded::Walked(tail.next_offset, tail.is_at_fill, SegmentFooter::empty())
        }
    }
}

/// Feed every held row to the resolver in key order, then hand each walked footer on
///
/// A key's versions keep segment order, so a tie still falls to the earlier source and
/// every key settles to the version it did. Ascending keys append, so each shard's tree
/// fills one leaf at a time and the open owes it no repack.
fn feed_held(
    held: &mut Vec<Held>,
    resolver: &mut Resolver<'_>,
    resumable: &mut [ResumableTail],
) -> Result<()> {
    let mut columns: Vec<ColumnId> = held
        .iter()
        .flat_map(|rows| rows.footer.partitions.iter().map(|partition| partition.column))
        .collect();
    columns.sort_unstable();
    columns.dedup();
    for column in columns {
        let mut cursors: Vec<Cursor<'_>> = held
            .iter()
            .enumerate()
            .filter_map(|(source, rows)| Cursor::open(source, rows, column))
            .collect::<Result<_>>()?;
        merge_cursors(&mut cursors, |cursor| cursor.feed(resolver))?;
    }
    for rows in held.drain(..) {
        if let Some(tail) = resumable.iter_mut().find(|tail| tail.segment == rows.segment) {
            tail.entries = rows.footer;
        }
    }
    Ok(())
}

/// One held partition walked in key order
struct Cursor<'a> {
    /// Position among the held segments, which breaks a tie between equal keys
    source: usize,
    segment: SegmentId,
    partition: &'a FooterPartition,

    /// Row numbers in key order, for rows that sit in arrival order
    order: Option<Vec<u32>>,
    at: usize,

    /// Range tombstone ends by row number
    ends: Vec<(u32, Option<KeyBytes>)>,
}

impl<'a> Cursor<'a> {
    fn open(source: usize, rows: &'a Held, column: ColumnId) -> Option<Result<Cursor<'a>>> {
        let at = rows
            .footer
            .partitions
            .iter()
            .position(|partition| partition.column == column)?;
        let partition = &rows.footer.partitions[at];
        Some(ends_of(rows, at).map(|ends| Cursor {
            source,
            segment: rows.segment,
            partition,
            order: (!rows.is_sorted).then(|| {
                // Equal keys keep arrival order, so the resolver meets them as it would
                // walking the tail.
                let mut order: Vec<u32> = (0..partition.len() as u32).collect();
                order.sort_unstable_by(|one, two| {
                    let ones = partition.key_at(*one as usize);
                    let twos = partition.key_at(*two as usize);
                    ones.cmp(&twos).then(one.cmp(two))
                });
                order
            }),
            at: 0,
            ends,
        }))
    }

    fn is_done(&self) -> bool {
        self.at >= self.partition.len()
    }

    fn row(&self) -> usize {
        match &self.order {
            Some(order) => order[self.at] as usize,
            None => self.at,
        }
    }

    fn key(&self) -> Option<&'a [u8]> {
        self.partition.key_at(self.row())
    }

    fn feed(&self, resolver: &mut Resolver<'_>) -> Result<()> {
        let row = self.row();
        let found = self.partition.row_at(row)?;
        let key = self.key().unwrap_or_default();
        let loc = Loc::new(self.segment, found.offset, found.len);
        match found.flags.is_range_tombstone() {
            true => {
                let end = self
                    .ends
                    .binary_search_by_key(&(row as u32), |(at, _)| *at)
                    .ok()
                    .and_then(|at| self.ends[at].1.clone());
                let start = RecordKey::from_bytes(self.partition.column, key)?;
                resolver.range(&start, end, found.lsn, loc);
            }
            false => resolver.put(
                self.partition.column,
                key,
                loc,
                found.lsn,
                found.flags.is_tombstone(),
            ),
        }
        Ok(())
    }
}

/// The ends of one partition's range tombstones, matched to their rows
fn ends_of(rows: &Held, partition: usize) -> Result<Vec<(u32, Option<KeyBytes>)>> {
    if rows.ends.is_empty() {
        return Ok(Vec::new());
    }
    let mut ends = rows.ends.iter();
    let mut matched = Vec::new();
    for (at, held) in rows.footer.partitions.iter().enumerate().take(partition + 1) {
        for row in 0..held.len() {
            if !held.row_at(row)?.flags.is_range_tombstone() {
                continue;
            }
            let end = ends.next().cloned().flatten();
            if at == partition {
                matched.push((row as u32, end));
            }
        }
    }
    Ok(matched)
}

/// Hand rows on across cursors in key order, a tie going to the earlier segment
fn merge_cursors(
    cursors: &mut Vec<Cursor<'_>>,
    mut take: impl FnMut(&Cursor<'_>) -> Result<()>,
) -> Result<()> {
    cursors.retain(|cursor| !cursor.is_done());
    // A binary heap of cursor positions with the least key on top.
    let mut heap: Vec<usize> = (0..cursors.len()).collect();
    for at in (0..heap.len() / 2).rev() {
        sift_down(&mut heap, at, cursors);
    }
    while let Some(&top) = heap.first() {
        take(&cursors[top])?;
        cursors[top].at += 1;
        if cursors[top].is_done() {
            heap.swap_remove(0);
        }
        sift_down(&mut heap, 0, cursors);
    }
    Ok(())
}

fn sift_down(heap: &mut [usize], mut at: usize, cursors: &[Cursor<'_>]) {
    let before = |one: usize, two: usize| match cursors[one].key().cmp(&cursors[two].key()) {
        std::cmp::Ordering::Equal => cursors[one].source < cursors[two].source,
        order => order == std::cmp::Ordering::Less,
    };
    loop {
        let mut least = at;
        for child in [2 * at + 1, 2 * at + 2] {
            if child < heap.len() && before(heap[child], heap[least]) {
                least = child;
            }
        }
        if least == at {
            return;
        }
        heap.swap(at, least);
        at = least;
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
    sealed: &[SealedSpan],
    index: &ReelIndex,
) -> Result<()> {
    let segments = index.segments();

    // A paged map holds only what the tails brought, so each entry is a suspect, and a
    // covered record goes now so it cannot stand in front of a newer sealed row.
    let mut suspects: HashMap<ColumnId, Vec<(KeyBytes, Entry)>> = HashMap::new();
    for spec in index.columns() {
        let Some(column) = index.column(spec.id) else {
            continue;
        };
        let mut rows = column.held();
        rows.retain(|(key, entry)| {
            let covered = !entry.is_grave() && column.is_covered_key(key.as_slice(), entry.lsn);
            if covered {
                column.drop_shadowed(key.as_slice(), segments);
            }
            !covered
        });
        if !rows.is_empty() {
            suspects.insert(spec.id, rows);
        }
    }
    if sealed_files.is_empty() || suspects.is_empty() {
        return Ok(());
    }

    // Only the segments whose span admits a suspect key are worth reopening.
    let mut probe: HashMap<SegmentId, Vec<ColumnId>> = HashMap::new();
    for span in sealed {
        let Some(rows) = suspects.get(&span.column) else {
            continue;
        };
        let admits = rows.iter().any(|(key, _)| {
            span.lowest.as_slice() <= key.as_slice() && key.as_slice() <= span.highest.as_slice()
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
                let rows = &suspects[&partition.column];
                let marks = shadowed.entry(partition.column).or_default();
                // The footer rows and the suspects are both in key order, so
                // one forward pass joins them.
                let mut at = 0usize;
                for row in partition.entries() {
                    let row = row?;
                    let key = row.key.as_slice();
                    while at < rows.len() && rows[at].0.as_slice() < key {
                        at += 1;
                    }
                    if at == rows.len() {
                        break;
                    }
                    let (suspect, entry) = &rows[at];
                    if suspect.as_slice() != key {
                        continue;
                    }
                    if row.lsn > entry.lsn {
                        marks.push(at);
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
                            .entry(at)
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
            if !pruned.contains(&at) {
                segments.shadow(segment, span);
            }
        }
    }

    // Take the shadowed entries out, booking a dropped record dead where it lies. A
    // dropped grave keeps its tombstone booking, which is bytes the segment holds.
    for (column, marks) in shadowed {
        let Some(index) = index.column(column) else {
            continue;
        };
        let rows = &suspects[&column];
        for at in marks {
            index.drop_shadowed(rows[at].0.as_slice(), segments);
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

/// Take a sealed segment's records from its footer, ranges from the ends beside it
///
/// Sealing waits for every reservation and syncs, so what a footer lists is what
/// landed and a batch frame has nothing left to decide. The one thing a footer
/// cannot answer is a range tombstone's end, and those arrive in footer order.
fn collect_partitions(
    segment: SegmentId,
    partitions: &[FooterPartition],
    ends: &mut impl Iterator<Item = Option<KeyBytes>>,
    resolver: &mut Resolver<'_>,
) -> Result<()> {
    for entry in partitions.iter().flat_map(|partition| partition.entries()) {
        let entry = entry?;
        let loc = Loc::new(segment, entry.offset, entry.len);
        match entry.is_range_tombstone() {
            true => resolver.range(&entry.key, ends.next().flatten(), entry.lsn, loc),
            false => resolver.put(
                entry.key.column,
                entry.key.as_slice(),
                loc,
                entry.lsn,
                entry.is_tombstone(),
            ),
        }
    }
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
    segment: SegmentId,
    footer: &SegmentFooter,
    ends: &mut impl Iterator<Item = Option<KeyBytes>>,
    resolver: &mut Resolver<'_>,
) -> Result<()> {
    resolver.book_tally(segment, footer.tally);
    resolver.index.segments().note_max(segment, footer.max_lsn);
    for partition in &footer.partitions {
        if let Some((lowest, highest)) = partition.key_range() {
            resolver.sealed.push(SealedSpan {
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
            let loc = Loc::new(segment, entry.offset, entry.len);
            if entry.is_range_tombstone() {
                resolver.range(&entry.key, ends.next().flatten(), entry.lsn, loc);
                continue;
            }
            resolver.see(entry.lsn);
            match entry.is_tombstone() {
                true => resolver.index.segments().mark_held(
                    segment,
                    entry.lsn,
                    span_of(entry.key.width(), entry.len),
                ),
                false => resolver.index.segments().note_min(segment, entry.lsn),
            }
        }
    }
    Ok(())
}

/// Read back the end of every range tombstone the footer lists, in its own order
///
/// A footer row says where its record sits and not where its range stops, so the ends
/// are the one thing a sealed segment still owes the medium. Taken here so the join
/// that follows reads nothing at all.
fn read_range_ends(
    driver: &IoDriver,
    file: FileId,
    footer: &SegmentFooter,
) -> Result<Vec<Option<KeyBytes>>> {
    let mut ends = Vec::new();
    for partition in &footer.partitions {
        for at in 0..partition.len() {
            let row = partition.row_at(at)?;
            if !row.flags.is_range_tombstone() {
                continue;
            }
            let width = partition.key_at(at).map_or(0, |key| key.len() as u16);
            ends.push(read_range_end(driver, file, width, row.offset, row.len)?);
        }
    }
    Ok(ends)
}

/// Read the exclusive end a range tombstone carries as its payload
fn read_range_end(
    driver: &IoDriver,
    file: FileId,
    key_width: u16,
    offset: u32,
    len: u32,
) -> Result<Option<KeyBytes>> {
    if len == 0 {
        return Ok(None);
    }
    let at = u64::from(offset) + HEADER_LEN as u64 + u64::from(key_width);
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

    /// Whether the walk stopped on bytes nothing has written
    pub is_at_fill: bool,
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
    let mut records = Vec::new();
    let (next_offset, is_at_fill) =
        walk_each(reader, segment, from, to, |record| records.push(record))?;
    Ok(Walked {
        records,
        next_offset,
        is_at_fill,
    })
}

/// An unsealed tail's walk, its rows packed the way the tail's own footer packs them
struct WalkedTail {
    /// Rows column by column, each column in the order the records sit in the file
    footer: SegmentFooter,

    /// Each range tombstone's end, in the footer's order
    ends: Vec<Option<KeyBytes>>,

    /// Offset the walk stopped at, which is where the tail resumes
    next_offset: u64,

    /// Whether the walk stopped on bytes nothing has written
    is_at_fill: bool,
}

/// Walk a whole tail into the footer an appender takes it up with
///
/// A record held whole costs four times its packed row, and the tails of a volume of
/// small records hold millions of them.
fn walk_tail(reader: &mut SegmentReader<'_>, segment: SegmentId, to: u64) -> Result<WalkedTail> {
    let mut footer = SegmentFooter::empty();
    let mut ends = Vec::new();
    let (next_offset, is_at_fill) = walk_each(reader, segment, 0, to, |record| {
        if record.flags.is_range_tombstone() {
            ends.push((record.key.column, record.range_end));
        }
        let (offset, len) = (record.loc.offset, record.loc.len);
        // The first record says what the rest of the tail likely holds, so the rows get
        // their room in one go.
        let span = HEADER_LEN as u64 + record.key.width() as u64 + u64::from(len);
        let is_first = footer.is_empty();
        footer.push(&FooterEntry::new(record.key, record.lsn, offset, len, record.flags));
        if is_first {
            footer.reserve_rows((to.saturating_sub(u64::from(offset)) / span) as usize);
        }
    })?;
    // The footer holds a column's rows together, and a stable sort keeps each column's
    // ends in file order.
    ends.sort_by_key(|(column, _)| *column);
    Ok(WalkedTail {
        footer,
        ends: ends.into_iter().map(|(_, end)| end).collect(),
        next_offset,
        is_at_fill,
    })
}

/// The walk itself, handing each record on once it is vetted and its batch is whole
///
/// Answers the offset the walk stopped at and whether it stopped on unwritten bytes.
fn walk_each(
    reader: &mut SegmentReader<'_>,
    segment: SegmentId,
    from: u64,
    to: u64,
    mut take: impl FnMut(WalkedRecord),
) -> Result<(u64, bool)> {
    let limit = reader.limit().min(to);
    let mut run = Vec::new();
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
            match walk_batch(reader, segment, &frame, offset, ends_at, &mut run)? {
                Batch::Whole => run.drain(..).for_each(&mut take),
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
        take(resolve_walked(reader, segment, header, at)?);
        next_offset = at + span;
    }

    // A segment is written through with zeros when it is made, and the fill reads back
    // as a data record with no sequence number. So a walk that stops on unwritten bytes
    // stops at the end of what was written, and one that stops on anything else has
    // written bytes ahead of it that an appender must not land behind.
    let is_at_fill = matches!(read_head(reader, next_offset, limit)?, Head::Missing);
    Ok((next_offset, is_at_fill))
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
    /// Every record the frame declared is there and verifies, held in the run
    Whole,

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
    run: &mut Vec<WalkedRecord>,
) -> Result<Batch> {
    run.clear();
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
        true => Ok(Batch::Whole),
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

/// Newest-wins resolution through the index's own sequence number guard, a batch at a time
struct Resolver<'a> {
    index: &'a ReelIndex,
    queue: KeyQueue,
    sealed: Vec<SealedSpan>,
    sealed_keys: HashMap<ColumnId, SealedKeys>,
    highest: Lsn,
    pages: bool,
}

impl<'a> Resolver<'a> {
    fn new(index: &'a ReelIndex, pages: bool) -> Resolver<'a> {
        index.clear();
        Resolver {
            index,
            queue: KeyQueue::default(),
            sealed: Vec::new(),
            sealed_keys: HashMap::new(),
            highest: Lsn::NONE,
            pages,
        }
    }

    fn see(&mut self, lsn: Lsn) {
        if lsn > self.highest {
            self.highest = lsn;
        }
    }

    /// Queue one record or point tombstone for its column's map
    fn put(&mut self, column: ColumnId, key: &[u8], loc: Loc, lsn: Lsn, is_delete: bool) {
        self.see(lsn);
        if column != self.queue.column || self.queue.rows.len() == BATCH {
            self.flush();
            self.queue.column = column;
        }
        self.queue.keys.extend_from_slice(key);
        let end = self.queue.keys.len();
        self.queue.rows.push((end, loc, lsn, is_delete));
    }

    /// Stand one range as a cover, after everything queued ahead of it
    fn range(&mut self, start: &RecordKey, end: Option<KeyBytes>, lsn: Lsn, tombstone: Loc) {
        self.flush();
        self.see(lsn);
        if let Some(column) = self.index.column(start.column) {
            column.remove_range(start.as_slice(), end.as_ref().map(KeyBytes::as_slice), lsn);
        }
        let span = span_of(start.width(), tombstone.len);
        self.index
            .segments()
            .mark_held(tombstone.segment, lsn, span);
    }

    /// Book what a sealed segment weighed when it closed
    fn book_tally(&mut self, segment: SegmentId, tally: FooterTally) {
        let bytes = SegmentBytes {
            live: tally.live,
            dead: tally.dead,
            ..SegmentBytes::default()
        };
        self.index.segments().adopt(segment, bytes);
    }

    /// Take a persisted index's counters for a segment, its winners booked as its rows land
    fn adopt_segment(&mut self, stamp: &PersistedSegment) {
        let bytes = SegmentBytes {
            live: stamp.held,
            dead: stamp.dead,
            held: stamp.held,
            held_lsn: stamp.held_lsn,
        };
        self.index.segments().adopt(stamp.segment, bytes);
        if let Some(min) = stamp.min_lsn {
            self.index.segments().note_min(stamp.segment, min);
        }
    }

    fn flush(&mut self) {
        self.queue.flush(self.index);
    }

    /// Settle a resident rebuild's covers and graves and hand back the highest sequence number
    fn finish(mut self) -> Result<Lsn> {
        self.flush();
        if !self.pages {
            while self.index.sweep_covers(usize::MAX)? {}
            self.index.prune_tombstones(Lsn(u64::MAX));
        }
        self.index.finish_rebuild(self.sealed, self.sealed_keys);
        Ok(self.highest)
    }
}

/// Records queued for one column's map, keys packed end to end
#[derive(Default)]
struct KeyQueue {
    column: ColumnId,
    keys: Vec<u8>,
    rows: Vec<(usize, Loc, Lsn, bool)>,
    landed: Vec<Landed>,
}

impl KeyQueue {
    /// Apply what is queued in order, a shard lock a run of keys
    fn flush(&mut self, index: &ReelIndex) {
        if self.rows.is_empty() {
            return;
        }
        if let Some(column) = index.column(self.column) {
            let mut start = 0;
            let moves: Vec<KeyMove<'_>> = self
                .rows
                .iter()
                .map(|&(end, loc, lsn, is_delete)| {
                    let key = &self.keys[start..end];
                    start = end;
                    KeyMove {
                        column: self.column,
                        key,
                        loc,
                        lsn,
                        is_delete,
                    }
                })
                .collect();
            self.landed.clear();
            column.apply_moves(&moves, index.segments(), &mut self.landed);
        }
        self.keys.clear();
        self.rows.clear();
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
    // An aligned write can land the footer with a block's worth of zeros after it, so
    // the trailer is read at the last byte that is not padding.
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
    match SegmentFooter::parse_owned(footer_bytes) {
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

    use std::path::PathBuf;
    use std::sync::Arc;

    use crate::units::ByteCount;

    use crate::append::admission::InflightBudget;
    use crate::append::{Appender, BatchRecord, BatchWrite, Commit};
    use crate::config::{
        IndexResidency, Preallocate, ReelConfig, ShardShapes, SyncPolicy, DEFAULT_FD_CACHE,
    };
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
            purge_mark: None,
            codec: Codec::None,
            map_shape: MapShape::Tree,
        },
        ColumnSpec {
            id: META,
            name: "meta",
            key_width: KeyWidth::Fixed(32),
            shard_bytes: 1,
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

    /// A rebuild's report and the index it filled
    struct Rebuilt {
        reel: RebuiltReel,
        index: ReelIndex,
    }

    impl std::ops::Deref for Rebuilt {
        type Target = RebuiltReel;

        fn deref(&self) -> &RebuiltReel {
            &self.reel
        }
    }

    impl Rebuilt {
        fn rows(&self, column: ColumnId) -> Vec<(KeyBytes, Entry)> {
            self.index
                .column(column)
                .map(|index| index.held())
                .unwrap_or_default()
        }
    }

    fn rebuild_on(sim: &SimIo, residency: IndexResidency) -> Rebuilt {
        let driver = IoDriver::new(Arc::new(sim.clone()));
        let index = ReelIndex::new(COLUMNS, residency, ShardShapes::Tree).expect("index");
        let pages = residency.pages();
        let reel = rebuild_reel(&driver, &[PathBuf::from(REEL_DIR)], &[false], pages, &index)
            .expect("rebuild");
        Rebuilt { reel, index }
    }

    fn rebuild(sim: &SimIo) -> Rebuilt {
        rebuild_on(sim, IndexResidency::Resident)
    }

    fn count(rebuilt: &Rebuilt) -> usize {
        rebuilt.rows(RECORDS).len() + rebuilt.rows(META).len()
    }

    fn keys_of(rebuilt: &Rebuilt, column: ColumnId) -> Vec<Vec<u8>> {
        rebuilt
            .rows(column)
            .iter()
            .map(|(key, _)| key.as_slice().to_vec())
            .collect()
    }

    // an overwrite-heavy segment keeps each key's newest version and books the rest dead
    #[test]
    fn overwrites_resolve_to_their_newest() {
        const KEYS: u8 = 8;
        const VERSIONS: usize = 16;
        let sim = SimIo::new(FaultPlan::new(1));
        let shared = shared(config(SyncPolicy::Never), &sim);
        let appender = Appender::open(Arc::clone(&shared), 0, None).expect("open");
        for _ in 0..VERSIONS {
            for byte in 0..KEYS {
                appender
                    .append_data(key(byte), vec![byte; 400], 0, Commit::PerRecord)
                    .expect("put");
            }
        }
        appender.seal().expect("seal");

        let rebuilt = rebuild(&sim);

        let rows = rebuilt.rows(RECORDS);
        assert_eq!(rows.len(), KEYS as usize);
        let newest = ((VERSIONS - 1) * KEYS as usize) as u64;
        for (_, entry) in &rows {
            assert!(entry.lsn > Lsn(newest), "each key kept its newest version");
        }
        let span = span_of(34, 400);
        let bytes = rebuilt.index.segment_bytes(SegmentId(1));
        assert_eq!(bytes.dead, ((VERSIONS - 1) * KEYS as usize) as u64 * span);
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
        let (_, entry) = rebuilt.rows(RECORDS)[0];
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
        let rebuilt = rebuild_on(&torn, IndexResidency::Paged);

        let rows = rebuilt.rows(RECORDS);
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
            let rows = rebuilt.rows(RECORDS);
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
}
