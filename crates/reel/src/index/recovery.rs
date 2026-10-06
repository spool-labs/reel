//! Rebuilding the reel's resident index from its segment files on open
//!
//! The files are the truth and the index is a cache, so on open the reel is read
//! back from disk. A sealed segment is read from the packed sorted footer at its end.
//! An unsealed tail is read through the journal beside it, a group of rows a write:
//! a group is kept only when every record it lists sits where its row says and checks
//! out, so a batch comes back whole or not at all. A file whose segment header carries
//! an unknown format or another segment number is quarantined. Each key resolves to its
//! highest sequence number, whichever segment carried it.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::mpsc::Receiver;
use std::sync::{Condvar, Mutex};

use crate::config::ThreadBudget;
use crate::error::Result;
use crate::format::column::{ColumnId, KeyBytes, RecordKey};
use crate::format::footer::{
    FooterEntry, FooterPartition, FooterTally, SegmentFooter, FIXED_TAIL_LEN,
};
use crate::format::journal::{journal_path, read_groups, JournalRow, JOURNAL_SUFFIX};
use crate::format::loc::{Loc, SegmentId};
use crate::format::lsn::Lsn;
use crate::format::record::{
    check_keyless, read_u32_le, Flags, KeylessRead, RecordHeader, RecordLayout, HEADER_LEN, KEYLESS_PREFIX,
};
use crate::format::segment_header::{SegmentHeader, FORMAT_VERSION};
use crate::index::column::{KeyMove, Landed};
use crate::index::counters::{Bookings, SegmentBytes, Tally};
use crate::index::entry::{span_of, Entry};
use crate::index::map::ReelIndex;
use crate::index::persisted::{trusted, PersistedReader, PersistedSegment};
use crate::io::op::FileId;
use crate::io::ServingBackend;
use crate::reel::segment::{read_segment_header, IoDriver, SegmentReader};
use crate::reel::segment_number;
use crate::sync::lock;

/// Bytes at the very end of a sealed segment holding its footer length and magic
const TRAILER_LEN: u64 = 8;

/// Bytes of the file end searched for the trailer past any aligned-write zeros
const TRAILER_PROBE_LEN: u64 = 4096;

/// Threads a rebuild opens segment files on, however wide the machine is
const MAX_READERS: usize = 8;

/// Segment files each reader may read ahead of the join on a resident rebuild
const READ_AHEAD: usize = 4;

/// Records one column batch takes into the index at a time
const BATCH: usize = 4096;

/// Segments a resident rebuild holds before it feeds their rows in key order
const FEED_WINDOW: usize = MAX_READERS * READ_AHEAD;

/// Threads a paged open loads sealed footers into the spot index on
const LOADERS: usize = 8;

/// Rows a column's window must hold before its feed splits across threads
const SPLIT_FEED_ROWS: usize = 4 * BATCH;

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

    /// Bytes of each tail's journal the rebuild consumed, and SEALED for a segment
    /// read from its footer
    pub consumed: HashMap<SegmentId, u64>,

    /// Unsealed tails and their file lengths. A crash keeps their reservation's
    /// blocks claimed past the end, and a writable open gives those back.
    pub walked: Vec<(PathBuf, u64)>,

    /// The same tails as appenders can pick them up, lowest number first
    pub resumable: Vec<ResumableTail>,

    /// A column the segments hold that this open doesn't declare
    pub undeclared: Option<ColumnId>,
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
            // Sealed and vouched for, so the rows the persisted index holds stand in
            // for reading it.
            consumed.insert(segment, SEALED);
            continue;
        }
        jobs.push((segment, path, len));
    }
    let mut held: Vec<Held> = Vec::new();
    // A paged open hands each sealed footer to the spot index's loaders as it is swept, so
    // the loads run beside the reads.
    // One footer waits for the loaders and each reader reads one ahead: the loaders are
    // the slower side, so anything deeper only holds footers, 0.6 GiB of a 100M reopen.
    let (queue, feed) = std::sync::mpsc::sync_channel::<(SegmentId, SegmentFooter)>(1);
    let feed = Mutex::new(feed);
    let loaders = match pages {
        true => LOADERS,
        false => 0,
    };
    std::thread::scope(|scope| -> Result<()> {
        let loading: Vec<_> = (0..loaders)
            .map(|_| scope.spawn(|| load_footers(index, &feed)))
            .collect();
        let queue = pages.then_some(queue);
        let mut is_sized = false;
        let ahead = match pages {
            true => 1,
            false => READ_AHEAD,
        };
        let read = read_segments(driver, &jobs, ahead, |at, parts| {
        let (segment, path, len) = &jobs[at];
        match absorb_segment(*segment, parts, pages, &mut resolver, &mut held)? {
            Loaded::Sealed(footer) => {
                consumed.insert(*segment, SEALED);
                sealed_files.push((*segment, path.clone(), *len));
                if let (Some(queue), Some(footer)) = (&queue, footer) {
                    if !is_sized {
                        index.reserve_fast(&footer, jobs.len());
                        is_sized = true;
                    }
                    // The loaders keep receiving until the queue closes, so a send never waits on nothing.
                    let _ = queue.send((*segment, footer));
                }
            }
            Loaded::Journaled(end) => {
                consumed.insert(*segment, end.journal_len);
                walked.push((path.clone(), *len));
                // A segment with no journal had its seal finish once, so an appender
                // must not write into it again whatever its footer reads as now.
                if let Some(rows) = end.rows {
                    resumable.push(ResumableTail {
                        segment: *segment,
                        path: path.clone(),
                        end: end.next_offset,
                        entries: SegmentFooter::empty(),
                        rows,
                    });
                }
            }
            Loaded::Foreign => quarantined.push(path.clone()),
        }
        if held.len() >= FEED_WINDOW {
            feed_held(&mut held, &mut resolver, &mut resumable)?;
        }
        Ok(())
    });
        drop(queue);
        for loader in loading {
            loader
                .join()
                .unwrap_or_else(|panic| std::panic::resume_unwind(panic))?;
        }
        read
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
    let undeclared = resolver.queue.undeclared;
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
        undeclared,
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

/// Take sealed footers off the queue into the spot index until it closes
///
/// A failed load keeps receiving, so the sweep feeding the queue never waits on a loader
/// that stopped, and the first error comes back once the queue is done.
fn load_footers(index: &ReelIndex, feed: &Mutex<Receiver<(SegmentId, SegmentFooter)>>) -> Result<()> {
    let mut failed = None;
    loop {
        let next = lock(feed).recv();
        let Ok((segment, footer)) = next else {
            break;
        };
        if failed.is_none() {
            failed = index.take_sealed_footer(segment, &footer).err();
        }
    }
    match failed {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

/// What reading one segment during a rebuild turned out to be
enum Loaded {
    /// A sealed segment, read from its footer, which a paged open still has to load
    Sealed(Option<SegmentFooter>),

    /// An unsealed tail read through its journal
    Journaled(JournaledEnd),

    /// A file that is not a segment of this reel
    Foreign,
}

/// An unsealed tail an appender can pick up where it stopped
///
/// The end is where its last accepted record ends. The footer is rebuilt from the
/// journal's rows, and the rows themselves go down again as the tail's fresh journal.
pub struct ResumableTail {
    pub segment: SegmentId,
    pub path: PathBuf,
    pub end: u64,
    pub entries: SegmentFooter,
    pub rows: Vec<JournalRow>,
}

/// What a journaled tail's read leaves for the rebuild beside its rows
struct JournaledEnd {
    /// Where the last accepted record ends
    next_offset: u64,

    /// Bytes of whole groups in the journal, where a follower picks up
    journal_len: u64,

    /// The accepted rows, for the tail that resumes them, nothing for a segment no
    /// appender may write into again
    rows: Option<Vec<JournalRow>>,
}

/// One segment file read off the medium, before any of it is joined
///
/// Everything the join needs is in here, so absorbing a segment touches no
/// descriptor and the reads can run wherever there is a thread for them.
enum SegmentParts {
    /// A sealed segment's footer, and the range ends its rows do not carry
    Sealed(SegmentFooter, Vec<Option<KeyBytes>>),

    /// An unsealed tail read through its journal
    Journaled(JournaledTail),

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
    ahead: usize,
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
    let window = readers * ahead;
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
    let read = read_parts(driver, file, path, segment, file_len);
    driver.close(file)?;
    read
}

fn read_parts(
    driver: &IoDriver,
    file: FileId,
    path: &Path,
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
        None => Ok(SegmentParts::Journaled(read_journaled(
            driver, file, path, file_len,
        )?)),
    }
}

/// Fold one segment's parts into the index, holding the rows it installs for the feed
///
/// A paging volume sweeps a sealed footer and installs none of its keys. Ranges stand at
/// once, since a cover settles by sequence number whichever rows it meets first. A
/// journaled tail's footer goes to the tail resuming it once its window is fed.
fn absorb_segment(
    segment: SegmentId,
    parts: SegmentParts,
    pages: bool,
    resolver: &mut Resolver<'_>,
    held: &mut Vec<Held>,
) -> Result<Loaded> {
    match parts {
        SegmentParts::Foreign => Ok(Loaded::Foreign),
        SegmentParts::Sealed(footer, ends) => match pages {
            true => {
                sweep_footer(segment, &footer, &mut ends.into_iter(), resolver)?;
                Ok(Loaded::Sealed(Some(footer)))
            }
            false => {
                stand_ranges(segment, &footer, ends, resolver)?;
                held.push(Held {
                    segment,
                    footer,
                    is_sorted: true,
                });
                Ok(Loaded::Sealed(None))
            }
        },
        SegmentParts::Journaled(tail) => {
            stand_ranges(segment, &tail.footer, tail.ends, resolver)?;
            held.push(Held {
                segment,
                footer: tail.footer,
                is_sorted: false,
            });
            Ok(Loaded::Journaled(JournaledEnd {
                next_offset: tail.next_offset,
                journal_len: tail.journal_len,
                rows: tail.rows,
            }))
        }
    }
}

/// Stand a segment's range tombstones, each with its end in footer order
fn stand_ranges(
    segment: SegmentId,
    footer: &SegmentFooter,
    ends: Vec<Option<KeyBytes>>,
    resolver: &mut Resolver<'_>,
) -> Result<()> {
    if ends.is_empty() {
        return Ok(());
    }
    let mut ends = ends.into_iter();
    for partition in &footer.partitions {
        for row in 0..partition.len() {
            let found = partition.row_at(row)?;
            if !found.flags.is_range_tombstone() {
                continue;
            }
            let start =
                RecordKey::from_bytes(partition.column, partition.key_at(row).unwrap_or_default())?;
            let loc = Loc::new(segment, found.offset, found.len);
            resolver.range(&start, ends.next().flatten(), found.lsn, loc);
        }
    }
    Ok(())
}

/// One segment's rows held until its window is fed in key order
struct Held {
    segment: SegmentId,
    footer: SegmentFooter,

    /// Whether the rows sit in key order, which a sealed footer's do and a walk's do not
    is_sorted: bool,
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
        .flat_map(|rows| {
            rows.footer
                .partitions
                .iter()
                .map(|partition| partition.column)
        })
        .collect();
    columns.sort_unstable();
    columns.dedup();
    for column in columns {
        // A walked partition is put in key order once, and every part reads that.
        let orders: Vec<Option<Vec<u32>>> =
            held.iter().map(|rows| key_order(rows, column)).collect();
        let cursors: Vec<Cursor<'_>> = held
            .iter()
            .zip(&orders)
            .enumerate()
            .filter_map(|(source, (rows, order))| {
                Cursor::open(source, rows, column, order.as_deref())
            })
            .collect();
        feed_column(column, cursors, resolver)?;
    }
    for rows in held.drain(..) {
        if let Some(tail) = resumable
            .iter_mut()
            .find(|tail| tail.segment == rows.segment)
        {
            tail.entries = rows.footer;
        }
    }
    Ok(())
}

/// A walked partition's rows in key order, equal keys keeping arrival order
fn key_order(rows: &Held, column: ColumnId) -> Option<Vec<u32>> {
    if rows.is_sorted {
        return None;
    }
    let partition = rows.footer.partition(column)?;
    let mut order: Vec<u32> = (0..partition.len() as u32).collect();
    order.sort_unstable_by(|one, two| {
        let ones = partition.key_at(*one as usize);
        let twos = partition.key_at(*two as usize);
        ones.cmp(&twos).then(one.cmp(two))
    });
    Some(order)
}

/// Feed one column's held rows, split across threads by whole shards once the window is big enough
fn feed_column(
    column: ColumnId,
    mut cursors: Vec<Cursor<'_>>,
    resolver: &mut Resolver<'_>,
) -> Result<()> {
    let rows: usize = cursors.iter().map(Cursor::left).sum();
    let target = resolver.index.column(column);
    let parts = match target {
        Some(index) if rows >= SPLIT_FEED_ROWS => {
            split_by_shard(index.shard_bytes(), ThreadBudget::Auto.resolve())
        }
        _ => Vec::new(),
    };
    if parts.len() < 2 {
        return merge_cursors(&mut cursors, |cursor| cursor.feed(resolver));
    }
    // Rows queued ahead of this window land before it, as they would on one thread.
    resolver.flush();
    let index = resolver.index;
    let cursors = &cursors;
    let highest = std::thread::scope(|scope| -> Result<Lsn> {
        let workers: Vec<_> = parts
            .iter()
            .map(|(low, high)| {
                scope.spawn(move || {
                    let bounds = (low.as_deref(), high.as_deref());
                    feed_part(index, column, cursors, bounds)
                })
            })
            .collect();
        let mut highest = Lsn::NONE;
        for worker in workers {
            let found = worker
                .join()
                .unwrap_or_else(|panic| std::panic::resume_unwind(panic))?;
            highest = highest.max(found);
        }
        Ok(highest)
    })?;
    resolver.see(highest);
    Ok(())
}

/// Apply the rows of one part, handing back the newest sequence number put
fn feed_part(
    index: &ReelIndex,
    column: ColumnId,
    cursors: &[Cursor<'_>],
    (low, high): (Option<&[u8]>, Option<&[u8]>),
) -> Result<Lsn> {
    let mut mine: Vec<Cursor<'_>> = cursors
        .iter()
        .map(|cursor| cursor.within(low, high))
        .collect();
    let mut queue = KeyQueue {
        column,
        ..KeyQueue::default()
    };
    // Booked here and handed over once, so the parts never meet on a segment's row.
    let tally = Tally::new(index.segments());
    let mut highest = Lsn::NONE;
    merge_cursors(&mut mine, |cursor| {
        cursor.put_with(|key, loc, lsn, is_delete| {
            highest = highest.max(lsn);
            queue.push(index, &tally, key, loc, lsn, is_delete);
        })
    })?;
    queue.flush_into(index, &tally);
    tally.settle();
    Ok(highest)
}

/// A part's key bounds, the low one inside it and the high one past it, open where absent
type Part = (Option<Vec<u8>>, Option<Vec<u8>>);

/// Values a leading key byte can take
const LEADING_BYTES: usize = 1 << 8;

/// Cut a column's keys into a part per thread at even steps of the leading byte, which keeps shards whole
fn split_by_shard(shard_bytes: u8, threads: usize) -> Vec<Part> {
    let parts = threads.min(LEADING_BYTES);
    if shard_bytes == 0 || parts < 2 {
        return Vec::new();
    }
    let mut split = Vec::with_capacity(parts);
    let mut low = None;
    for at in 1..parts {
        let cut = vec![(LEADING_BYTES * at / parts) as u8];
        split.push((low, Some(cut.clone())));
        low = Some(cut);
    }
    split.push((low, None));
    split
}

/// One held partition walked in key order, over a span of its places
#[derive(Clone, Copy)]
struct Cursor<'a> {
    /// Position among the held segments, which breaks a tie between equal keys
    source: usize,
    segment: SegmentId,
    partition: &'a FooterPartition,

    /// Row numbers in key order, for rows that sit in arrival order
    order: Option<&'a [u32]>,
    at: usize,

    /// One past the last place this cursor walks
    end: usize,
}

impl<'a> Cursor<'a> {
    fn open(
        source: usize,
        rows: &'a Held,
        column: ColumnId,
        order: Option<&'a [u32]>,
    ) -> Option<Cursor<'a>> {
        let partition = rows.footer.partition(column)?;
        Some(Cursor {
            source,
            segment: rows.segment,
            partition,
            order,
            at: 0,
            end: partition.len(),
        })
    }

    /// The same places, from the first key at or past `low` to the last one below `high`
    fn within(&self, low: Option<&[u8]>, high: Option<&[u8]>) -> Cursor<'a> {
        let at = low.map_or(self.at, |low| self.seek(low));
        let end = high.map_or(self.end, |high| self.seek(high)).max(at);
        Cursor { at, end, ..*self }
    }

    /// The first place whose key is not below a bound
    fn seek(&self, bound: &[u8]) -> usize {
        let (mut low, mut high) = (self.at, self.end);
        while low < high {
            let middle = low + (high - low) / 2;
            match self.key_of(middle) < bound {
                true => low = middle + 1,
                false => high = middle,
            }
        }
        low
    }

    fn is_done(&self) -> bool {
        self.at >= self.end
    }

    fn left(&self) -> usize {
        self.end.saturating_sub(self.at)
    }

    fn row_of(&self, at: usize) -> usize {
        match self.order {
            Some(order) => order[at] as usize,
            None => at,
        }
    }

    fn row(&self) -> usize {
        self.row_of(self.at)
    }

    fn key(&self) -> Option<&'a [u8]> {
        self.partition.key_at(self.row())
    }

    fn key_of(&self, at: usize) -> &'a [u8] {
        self.partition.key_at(self.row_of(at)).unwrap_or_default()
    }

    /// Hand the row at this place to `put`, a range having stood when its segment was held
    fn put_with(&self, put: impl FnOnce(&'a [u8], Loc, Lsn, bool)) -> Result<()> {
        let found = self.partition.row_at(self.row())?;
        if !found.flags.is_range_tombstone() {
            let key = self.key().unwrap_or_default();
            let loc = Loc::new(self.segment, found.offset, found.len);
            put(key, loc, found.lsn, found.flags.is_tombstone());
        }
        Ok(())
    }

    fn feed(&self, resolver: &mut Resolver<'_>) -> Result<()> {
        let column = self.partition.column;
        self.put_with(|key, loc, lsn, is_delete| resolver.put(column, key, loc, lsn, is_delete))
    }
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
                // one forward pass joins them, and only a row whose key matches is
                // read past its key.
                let mut at = 0usize;
                for found in 0..partition.len() {
                    let key = partition.key_at(found).unwrap_or_default();
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
                    let row = partition.row_at(found)?;
                    if row.lsn > entry.lsn {
                        marks.push(at);
                    } else if row.lsn < entry.lsn
                        && entry.lsn >= footer.sealed_at
                        && !row.flags.is_tombstone()
                        && !row.flags.is_range_tombstone()
                    {
                        // At or past the frontier, so the tally froze without this
                        // shadowing. Below it the tally already counted the death and
                        // a debit here would count it twice.
                        let span = span_of(key.len() as u16, row.len);
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
    if let Some(partition) = footer
        .partitions
        .iter()
        .find(|held| resolver.index.column(held.column).is_none())
    {
        resolver.queue.undeclared.get_or_insert(partition.column);
    }
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

        // The table hears the partition once, with its oldest record and its
        // tombstones' spans summed, since a call a row took its lock a row.
        let mut oldest = Lsn(u64::MAX);
        let (mut held, mut held_lsn) = (0u64, Lsn::NONE);
        for at in 0..partition.len() {
            let row = partition.row_at(at)?;
            let key = partition.key_at(at).unwrap_or_default();
            if row.flags.is_range_tombstone() {
                let start = RecordKey::from_bytes(partition.column, key)?;
                let loc = Loc::new(segment, row.offset, row.len);
                resolver.range(&start, ends.next().flatten(), row.lsn, loc);
                continue;
            }
            resolver.see(row.lsn);
            match row.flags.is_tombstone() {
                true => {
                    held += span_of(key.len() as u16, row.len);
                    held_lsn = held_lsn.max(row.lsn);
                }
                false => oldest = oldest.min(row.lsn),
            }
        }
        if held > 0 {
            resolver.index.segments().mark_held(segment, held_lsn, held);
        }
        if oldest != Lsn(u64::MAX) {
            resolver.index.segments().note_min(segment, oldest);
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
    let mut layout = None;
    for partition in &footer.partitions {
        for at in 0..partition.len() {
            let row = partition.row_at(at)?;
            if !row.flags.is_range_tombstone() {
                continue;
            }
            let width = partition.key_at(at).map_or(0, |key| key.len());
            let layout = match layout {
                Some(layout) => layout,
                None => *layout.insert(
                    read_segment_header(driver, file)?.map_or(RecordLayout::Keyed, |header| header.layout),
                ),
            };
            ends.push(read_range_end(driver, file, layout.prefix_len(width, row.len), row.offset, row.len)?);
        }
    }
    Ok(ends)
}

/// Read the exclusive end a range tombstone carries as its payload
fn read_range_end(
    driver: &IoDriver,
    file: FileId,
    prefix: usize,
    offset: u32,
    len: u32,
) -> Result<Option<KeyBytes>> {
    if len == 0 {
        return Ok(None);
    }
    let at = u64::from(offset) + prefix as u64;
    let bytes = driver.pread(file, at, u64::from(len))?;
    if bytes.len() < len as usize {
        return Ok(None);
    }
    Ok(KeyBytes::new(&bytes).ok())
}

/// One record a follower applies, carrying everything applying it needs
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

/// The consumed position of a segment read from its footer, which a follower reads no more of
pub const SEALED: u64 = u64::MAX;

/// An unsealed tail read through its journal, its rows packed the way its own footer packs them
struct JournaledTail {
    /// Rows column by column, each column in the order its journal took them
    footer: SegmentFooter,

    /// Each range tombstone's end, in the footer's order
    ends: Vec<Option<KeyBytes>>,

    /// The accepted rows as the journal holds them, nothing where there is no journal
    rows: Option<Vec<JournalRow>>,

    /// Where the last accepted record ends, which is where the tail resumes
    next_offset: u64,

    /// Bytes of whole groups the journal holds
    journal_len: u64,
}

/// Read an unsealed tail through its journal, keeping each group whose records all check out
///
/// A group is one write, so a batch is kept whole or not at all, and a group listing a
/// record that did not land as its row says is dropped. A segment with no journal had
/// its seal finish once and its footer go bad since, and nothing lists its records.
fn read_journaled(driver: &IoDriver, file: FileId, path: &Path, file_len: u64) -> Result<JournaledTail> {
    let mut tail = JournaledTail {
        footer: SegmentFooter::empty(),
        ends: Vec::new(),
        rows: None,
        next_offset: header_end(driver, file)?,
        journal_len: 0,
    };
    let Some(bytes) = read_journal(driver, path, 0)? else {
        return Ok(tail);
    };
    let (groups, valid) = read_groups(&bytes);
    tail.journal_len = valid as u64;
    let layout = read_segment_header(driver, file)?.map_or(RecordLayout::Keyed, |header| header.layout);
    let mut reader = SegmentReader::new(driver, file, file_len);
    let mut rows = Vec::new();
    let mut ends = Vec::new();
    for group in groups {
        if !all_landed(&mut reader, layout, &group)? {
            continue;
        }
        for row in group {
            let span = span_of(row.key.width(), row.len);
            tail.next_offset = tail.next_offset.max(u64::from(row.offset) + span);
            if row.flags.is_range_tombstone() {
                ends.push((row.key.column, row.range_end.clone()));
            }
            tail.footer.push(&FooterEntry::new(row.key.clone(), row.lsn, row.offset, row.len, row.flags));
            rows.push(row);
        }
    }
    // The footer holds a column's rows together, and a stable sort keeps each column's
    // ends in journal order.
    ends.sort_by_key(|(column, _)| *column);
    tail.ends = ends.into_iter().map(|(_, end)| end).collect();
    tail.rows = Some(rows);
    Ok(tail)
}

/// Unlink every journal a footer has taken over, and every journal part a resume left
pub fn remove_stale_journals(driver: &IoDriver, root: &Path, consumed: &HashMap<SegmentId, u64>) -> Result<()> {
    for entry in driver.list_or_empty(root)? {
        let is_stale = match entry.name.strip_suffix(JOURNAL_SUFFIX) {
            Some(number) => number
                .parse()
                .ok()
                .is_none_or(|number| consumed.get(&SegmentId(number)).is_none_or(|at| *at == SEALED)),
            None => entry.name.ends_with(".rows.part"),
        };
        if is_stale {
            driver.unlink(&root.join(&entry.name))?;
        }
    }
    Ok(())
}

/// A segment's journal from an offset to its end, nothing where it has no journal
pub(crate) fn read_journal(driver: &IoDriver, segment_path: &Path, from: u64) -> Result<Option<Vec<u8>>> {
    let journal = match driver.open(&journal_path(segment_path), false) {
        Ok(journal) => journal,
        Err(error) if error.is_missing() => return Ok(None),
        Err(error) => return Err(error),
    };
    let read = driver.length(journal).and_then(|len| match len > from {
        true => driver.pread(journal, from, len - from),
        false => Ok(Vec::new()),
    });
    driver.close(journal)?;
    read.map(Some)
}

/// Where the segment header record ends, which is where an empty tail resumes
fn header_end(driver: &IoDriver, file: FileId) -> Result<u64> {
    let head = driver.pread(file, 0, HEADER_LEN as u64)?;
    let header = RecordHeader::unpack(head.get(..HEADER_LEN).unwrap_or(&[]))?;
    Ok(header.span())
}

/// Whether every record a group lists sits where its row says and checks out
fn all_landed(reader: &mut SegmentReader<'_>, layout: RecordLayout, group: &[JournalRow]) -> Result<bool> {
    for row in group {
        if !is_indexable(row.flags) {
            return Ok(false);
        }
        if let Some(check) = layout.keyless_key(row.len) {
            let span = KEYLESS_PREFIX + row.len as usize;
            let record = reader.range(u64::from(row.offset), span)?;
            if record.len() < span {
                return Ok(false);
            }
            let (prefix, payload) = record.split_at(KEYLESS_PREFIX);
            if !matches!(check_keyless(prefix, payload, row.key.as_ref(), row.flags, &check), KeylessRead::Intact(_)) {
                return Ok(false);
            }
            continue;
        }
        let prefix = HEADER_LEN + row.key.as_slice().len();
        let head = reader.range(u64::from(row.offset), prefix)?;
        if head.len() < prefix {
            return Ok(false);
        }
        let Ok(header) = RecordHeader::unpack(head) else {
            return Ok(false);
        };
        if header.key != row.key || header.lsn != row.lsn || header.length != row.len || header.flags != row.flags {
            return Ok(false);
        }
        if !verify_record(reader, u64::from(row.offset), &header)? {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Every record a sealed footer lists, as a follower applies it
pub(crate) fn footer_records(
    driver: &IoDriver,
    file: FileId,
    segment: SegmentId,
    footer: &SegmentFooter,
) -> Result<Vec<WalkedRecord>> {
    let mut ends = read_range_ends(driver, file, footer)?.into_iter();
    let mut records = Vec::with_capacity(footer.entry_count());
    for partition in &footer.partitions {
        for at in 0..partition.len() {
            let row = partition.row_at(at)?;
            let Some(key) = partition.key_at(at) else {
                continue;
            };
            let range_end = match row.flags.is_range_tombstone() {
                true => ends.next().flatten(),
                false => None,
            };
            records.push(WalkedRecord {
                key: RecordKey::from_bytes(partition.column, key)?,
                lsn: row.lsn,
                loc: Loc::new(segment, row.offset, row.len),
                flags: row.flags,
                range_end,
            });
        }
    }
    Ok(records)
}

/// The rows of a journal as a follower applies them
pub(crate) fn journal_records(segment: SegmentId, groups: Vec<Vec<JournalRow>>) -> Vec<WalkedRecord> {
    groups
        .into_iter()
        .flatten()
        .map(|row| WalkedRecord {
            loc: Loc::new(segment, row.offset, row.len),
            key: row.key,
            lsn: row.lsn,
            flags: row.flags,
            range_end: row.range_end,
        })
        .collect()
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
        if column != self.queue.column {
            self.flush();
            self.queue.column = column;
        }
        self.queue
            .push(self.index, self.index.segments(), key, loc, lsn, is_delete);
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
        self.index.finish_rebuild(self.sealed);
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
    undeclared: Option<ColumnId>,
}

impl KeyQueue {
    /// Queue one record or point tombstone, applying the batch ahead of it once full
    fn push<Book: Bookings>(
        &mut self,
        index: &ReelIndex,
        segments: &Book,
        key: &[u8],
        loc: Loc,
        lsn: Lsn,
        is_delete: bool,
    ) {
        if self.rows.len() == BATCH {
            self.flush_into(index, segments);
        }
        self.keys.extend_from_slice(key);
        let end = self.keys.len();
        self.rows.push((end, loc, lsn, is_delete));
    }

    /// Apply what is queued in order, booked straight into the segment table
    fn flush(&mut self, index: &ReelIndex) {
        self.flush_into(index, index.segments());
    }

    /// Apply what is queued in order, a shard lock a run of keys
    fn flush_into<Book: Bookings>(&mut self, index: &ReelIndex, segments: &Book) {
        if self.rows.is_empty() {
            return;
        }
        match index.column(self.column) {
            Some(column) => {
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
                column.apply_moves(&moves, segments, &mut self.landed);
            }
            // A column this open doesn't declare still holds bytes in its segments,
            // and nothing here can shadow them, so its rows are booked live.
            None => {
                self.undeclared.get_or_insert(self.column);
                let mut start = 0;
                for &(end, loc, lsn, is_delete) in &self.rows {
                    let span = span_of((end - start) as u16, loc.len);
                    start = end;
                    match is_delete {
                        true => segments.mark_held(loc.segment, lsn, span),
                        false => segments.mark_live(loc.segment, lsn, span),
                    }
                }
            }
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
    use crate::config::{IndexResidency, Preallocate, ReelConfig, SyncPolicy, DEFAULT_FD_CACHE};
    use crate::format::column::{Codec, ColumnSet, ColumnSpec, KeyWidth};
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
        },
        ColumnSpec {
            id: META,
            name: "meta",
            key_width: KeyWidth::Fixed(32),
            shard_bytes: 1,
            purge_mark: None,
            codec: Codec::None,
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
        let index = ReelIndex::new(COLUMNS, residency).expect("index");
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

    // a window split across threads keeps every key's newest version and books what one thread would
    #[test]
    fn a_split_feed_keeps_newest_versions() {
        const KEYS: u32 = 12_000;
        const DELETE_EVERY: usize = 7;
        let sim = SimIo::new(FaultPlan::new(1));
        let shared = shared(config(SyncPolicy::Never), &sim);
        let appender = Appender::open(Arc::clone(&shared), 0, None).expect("open");
        // The leading bytes are a hash of the number, so the keys cover every shard.
        let spread = |at: u32| {
            let mut bytes = [0u8; 34];
            bytes[..4].copy_from_slice(&at.wrapping_mul(0x9E37_79B9).to_be_bytes());
            bytes[4..8].copy_from_slice(&at.to_be_bytes());
            RecordKey::from_bytes(RECORDS, &bytes).expect("key")
        };
        let mut newest = HashMap::new();
        for version in 0..2u8 {
            for at in 0..KEYS {
                let put = appender
                    .append_data(spread(at), vec![version; 16], 0, Commit::PerRecord)
                    .expect("put");
                newest.insert(at, put.lsn);
            }
        }
        let deleted: Vec<u32> = (0..KEYS).step_by(DELETE_EVERY).collect();
        for at in &deleted {
            appender
                .append_tombstone(spread(*at), Commit::PerRecord)
                .expect("delete");
            newest.remove(at);
        }
        appender.seal().expect("seal");

        let driver = IoDriver::new(Arc::new(sim.clone()));
        let index = ReelIndex::new(COLUMNS, IndexResidency::Resident).expect("index");
        let roots = [PathBuf::from(REEL_DIR)];
        let rebuilt = rebuild_reel(&driver, &roots, &[false], false, &index).expect("rebuild");

        let rows = index
            .column(RECORDS)
            .map(|column| column.held())
            .unwrap_or_default();
        assert_eq!(rows.len(), newest.len());
        for (key, entry) in &rows {
            let at = u32::from_be_bytes(key.as_slice()[4..8].try_into().expect("number"));
            assert_eq!(
                Some(&entry.lsn),
                newest.get(&at),
                "key {at} kept its newest"
            );
        }
        let (mut live, mut dead) = (0, 0);
        for number in 1..=rebuilt.highest_segment.as_u32() {
            let bytes = index.segment_bytes(SegmentId(number));
            live += bytes.live;
            dead += bytes.dead;
        }
        let (record, tombstone) = (span_of(34, 16), span_of(34, 0));
        let deletes = deleted.len() as u64;
        assert_eq!(dead, (u64::from(KEYS) + deletes) * record);
        assert_eq!(live, newest.len() as u64 * record + deletes * tombstone);
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
        // A batch leaves its sync to the caller, and the sync is what journals its rows.
        appender.flush().expect("flush");

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
        appender.flush().expect("flush");
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
        appender.flush().expect("flush");
        corrupt_payload(&sim, u64::from(committed[0].loc.offset));

        let rebuilt = rebuild(&sim);

        assert_eq!(keys_of(&rebuilt, RECORDS), vec![vec![9u8; 34]]);
    }

    /// Write a batch of two behind one plain record, journal it, and say where its records sit
    fn tail_with_a_batch(sim: &SimIo) -> (u64, u64) {
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
        appender.flush().expect("flush");
        (u64::from(committed[0].loc.offset), u64::from(committed[1].loc.offset))
    }

    // a batch whose records never landed is dropped, frame and all
    #[test]
    fn a_batch_the_write_never_reached_is_dropped() {
        let sim = SimIo::new(FaultPlan::new(1));
        let (first, _) = tail_with_a_batch(&sim);
        // The shape a writev that stopped before the batch leaves: the run is the
        // reservation's own zeros.
        zero_from(&sim, first, 4096);

        let rebuilt = rebuild(&sim);

        assert_eq!(keys_of(&rebuilt, RECORDS), vec![vec![9u8; 34]]);
    }

    // a batch missing its last record is dropped rather than half applied
    #[test]
    fn a_batch_cut_short_is_dropped() {
        let sim = SimIo::new(FaultPlan::new(1));
        let (_, second) = tail_with_a_batch(&sim);
        zero_from(&sim, second, 4096);

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

    /// Cut a sealed segment back to its record region and put back the journal it held,
    /// the shape a seal that failed partway leaves
    fn strip_footer(image: &mut DurableImage, name: &str, journal: Vec<u8>) {
        let path = Path::new(REEL_DIR).join(name);
        image.push((journal_path(&path), journal));
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
        appender.flush().expect("flush");
        let journal = sim
            .durable_bytes(&journal_path(&Path::new(REEL_DIR).join("000001.reel")))
            .expect("journal");
        appender.seal().expect("seal one");
        appender
            .append_data(key(1), vec![0x33; 300], 0, Commit::PerRecord)
            .expect("new version");
        appender
            .append_data(key(3), vec![0x44; 300], 0, Commit::PerRecord)
            .expect("rewritten");
        appender.seal().expect("seal two");

        let mut image = sim.durable_image();
        strip_footer(&mut image, "000001.reel", journal);
        let torn = SimIo::from_image(image);
        let rebuilt = rebuild_on(&torn, IndexResidency::Paged);

        let rows = rebuilt.rows(RECORDS);
        assert!(
            !rows.iter().any(|(key, _)| key.as_slice() == [1u8; 34]),
            "the journaled old version shadows the sealed rewrite"
        );
        assert!(
            !rows.iter().any(|(key, _)| key.as_slice() == [3u8; 34]),
            "the journaled tombstone left a grave over the sealed rewrite"
        );
        assert!(
            rows.iter()
                .any(|(key, entry)| key.as_slice() == [2u8; 34] && !entry.is_grave()),
            "a key with no newer sealed version lost its only record to the prune"
        );
    }

    // a failed write lists no row, so records above it survive a reopen; the fault
    // position is searched for since the op count moves with the write path
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
            if appender.flush().is_err() {
                continue;
            }
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
        appender.flush().expect("flush");
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

    // a footer that rots after its seal leaves its rows unlisted, and no tail resumes into it
    #[test]
    fn a_rotted_footer_loses_its_rows() {
        let sim = SimIo::new(FaultPlan::new(1));
        sealed_pair(&sim);
        let length = segment_len(&sim, "000001.reel");
        let driver = IoDriver::new(Arc::new(sim.clone()));
        let file = open_created(&driver, &Path::new(REEL_DIR).join("000001.reel"));

        write_at(&driver, file, length - 1, &[0xff]);
        let rebuilt = rebuild(&sim);

        assert!(keys_of(&rebuilt, RECORDS).is_empty());
        assert!(
            !rebuilt.resumable.iter().any(|tail| tail.segment == SegmentId(1)),
            "a tail would write into a sealed segment"
        );
    }

    // a seal that stopped before its footer was whole reads back through the journal
    #[test]
    fn a_seal_cut_short_reads_its_journal() {
        let sim = SimIo::new(FaultPlan::new(1));
        let shared = shared(config(SyncPolicy::Never), &sim);
        let appender = Appender::open(Arc::clone(&shared), 0, None).expect("open");
        appender
            .append_data(key(1), vec![0x11; 400], 0, Commit::PerRecord)
            .expect("put");
        appender
            .append_data(key(2), vec![0x22; 600], 0, Commit::PerRecord)
            .expect("put");
        appender.flush().expect("flush");
        let journal = sim
            .durable_bytes(&journal_path(&Path::new(REEL_DIR).join("000001.reel")))
            .expect("journal");
        appender.seal().expect("seal");
        let mut image = sim.durable_image();
        let cut = footer_len_for(2);

        truncate_segment(&mut image, "000001.reel", cut);
        image.push((journal_path(&Path::new(REEL_DIR).join("000001.reel")), journal));
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
