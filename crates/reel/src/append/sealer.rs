//! The per-tail worker that seals rolled segments and runs owed flushes

use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::sync::{Arc, RwLock};
use std::thread::JoinHandle;

use crate::config::RepairPath;
use crate::error::Result;
use crate::format::footer::SegmentFooter;
use crate::format::loc::SegmentId;
use crate::format::record::{RecordHeader, HEADER_LEN};
use crate::io::op::{Advice, WriteBuf};
use crate::reel::ReelShared;
use crate::sync::tension::Tension;
use crate::sync::{lock, read, write};

use super::flush::Owed;
use super::Active;

/// Seal a segment, which closes it and leaves it readable and never written again
///
/// Free of the tail on purpose: on a full segment the seal is a sort, megabytes of io
/// and an fsync that the writer filling it would otherwise pay inside its own put.
pub(super) fn seal_segment(shared: &Arc<ReelShared>, active: &Active, end: u64) -> Result<()> {
    if active.terminal.load(Ordering::Acquire) {
        return Ok(());
    }
    let bridged = bridge_reserved_slack(shared, active, end)?;

    // Writeback orders nothing, so under the one-sync seal a crash can leave a durable
    // footer naming bytes that never landed. With peers that resolves to a read-time
    // miss and a repair; with none the second flush is the whole guarantee.
    if shared.config.repair == RepairPath::None {
        shared.driver.sync_full(active.handle.file())?;
    }

    let mut footer = std::mem::replace(&mut *lock(&active.entries), SegmentFooter::empty());
    // What the segment weighs, live against dead. A rebuild cannot work this out
    // without joining every segment on the volume.
    footer.tally = shared.tally_of(active.handle.id());
    // The frontier the tally is current to. Bookings from writes issued at or past this
    // are the reopen join's to settle, not the tally's.
    footer.sealed_at = shared.lsn.peek();
    let written = (|| -> Result<()> {
        let packed = footer.pack_fenced(
            shared.filter_bits_for(active.handle.id()),
            shared.config.seal_fences(),
        )?;
        shared
            .driver
            .writev_all(active.handle.file(), bridged, vec![WriteBuf::owned(packed)])?;
        shared.driver.sync_full(active.handle.file())
    })();
    if let Err(error) = written {
        // The footer is the one copy of what this segment holds, so a failed seal puts
        // it back for the retry the maintenance tick owes.
        *lock(&active.entries) = footer;
        return Err(error);
    }
    // The handle stops being a write head here and becomes the one readers find in the
    // cache, so it takes the hint a read-path open would have given it.
    let _ = shared
        .driver
        .advise(active.handle.file(), 0, 0, Advice::Random);
    shared.fd_cache.insert(active.handle.clone());
    // The footer is on disk now, so a paged index can take this segment's keys over from
    // the map. The tail does not hold the index, so it leaves the number instead.
    shared.note_sealed(active.handle.id());
    Ok(())
}

/// Seal a retired segment and say what it did to its durability
///
/// The order is load bearing: the segment is released only once its footer is down,
/// since releasing it is what makes it a compaction candidate.
pub(super) fn retire_segment(shared: &Arc<ReelShared>, retired: &Active, end: u64) -> Result<()> {
    let sealed = seal_segment(shared, retired, end);
    let is_terminal = retired.terminal.load(Ordering::Acquire);
    match &sealed {
        // A terminal segment's seal was skipped, not performed: nothing new went
        // durable, and the broken mark its failed sync left has to keep standing.
        Ok(()) if is_terminal => {}
        Ok(()) => retired.sync.mark_durable(),
        Err(_) => retired.sync.mark_broken(),
    }
    // What the segment still owed a sync when it broke. Counting it is what stops every
    // later flush answering clean over records the next open will not have.
    if (sealed.is_err() || is_terminal) && retired.sync.covered() < end {
        shared.note_past_saving();
    }
    // No footer went down on either of these. The release is what takes the holds off,
    // so the mark has to be on first.
    if sealed.is_err() || is_terminal {
        shared.note_unsealed(retired.handle.id());
    }
    // A failed non-terminal seal keeps the segment unreleased on purpose: a candidate
    // with no footer can be retired wholly dead while its seal waits parked, and the
    // retry would then seal a corpse and reinstall spans the index had forgotten.
    if sealed.is_ok() || is_terminal {
        shared.release_segment(retired.handle.id());
    }
    sealed
}

/// A rolled segment whose seal failed, parked for the maintenance tick
pub(crate) struct BrokenSeal {
    active: Active,
    end: u64,

    /// Whether the failure was counted past saving, so a success uncounts it
    counted: bool,
}

/// Park a segment whose seal failed, unless its tail is closing
///
/// A terminal segment's seal was skipped by design and closing takes the tick with it.
pub(super) fn park_broken_seal(shared: &Arc<ReelShared>, active: Active, end: u64) {
    if active.terminal.load(Ordering::Acquire) {
        return;
    }
    let counted = active.sync.covered() < end;
    lock(&shared.broken_seals).push(BrokenSeal {
        active,
        end,
        counted,
    });
}

/// Retry the seals of parked broken segments, from the maintenance tick
///
/// A success gives back the past-saving count the failure took, which is what lets
/// flush answer clean again.
pub(crate) fn retry_broken_seals(shared: &Arc<ReelShared>) -> usize {
    let parked = std::mem::take(&mut *lock(&shared.broken_seals));
    if parked.is_empty() {
        return 0;
    }
    let mut sealed = 0usize;
    for job in parked {
        // A tail that turned terminal while the segment was parked skips the seal rather
        // than performing it, so the hold goes back: nothing will seal this segment now.
        if job.active.terminal.load(Ordering::Acquire) {
            shared.release_segment(job.active.handle.id());
            continue;
        }
        match seal_segment(shared, &job.active, job.end) {
            Ok(()) => {
                job.active.sync.mark_durable();
                if job.counted {
                    shared.un_note_past_saving();
                }
                // A sealed segment rejoins the compaction candidates, footer first.
                shared.release_segment(job.active.handle.id());
                sealed += 1;
            }
            Err(error) => {
                tracing::warn!("a parked seal failed again: {error}");
                lock(&shared.broken_seals).push(job);
            }
        }
    }
    sealed
}

/// Stamp a failed reservation as fill, so recovery can walk past it
///
/// An unwritten span ends every recovery walk, and by the time a write reports its
/// error a neighbour may already have committed above the range, so the stamp turns the
/// span into one fill record a walk hops.
pub(super) fn stamp_failed_range(
    shared: &Arc<ReelShared>,
    active: &Active,
    base: u64,
    span: u64,
) -> bool {
    if span < HEADER_LEN as u64 {
        return false;
    }
    let pad = RecordHeader::fill((span - HEADER_LEN as u64) as u32);
    let mut bufs = Vec::with_capacity(1);
    WriteBuf::push_prefix(&mut bufs, pad.pack());
    // The count is the answer, not the error: a short write reports success with only a
    // prefix persisted, which is exactly the garbage the stamp exists to cover.
    match shared.driver.writev(active.handle.file(), base, bufs) {
        Ok(wrote) => wrote == HEADER_LEN as u64,
        Err(_) => false,
    }
}

/// Fill the gap between the last record and the reserved space with one pad
pub(super) fn bridge_reserved_slack(
    shared: &Arc<ReelShared>,
    active: &Active,
    end: u64,
) -> Result<u64> {
    let alloc_high = active.alloc_high.load(Ordering::Acquire);
    if alloc_high <= end {
        return Ok(end);
    }
    let gap = alloc_high - end;
    if gap < HEADER_LEN as u64 {
        return Ok(end);
    }
    let pad = RecordHeader::fill((gap - HEADER_LEN as u64) as u32);
    let mut bufs = Vec::with_capacity(1);
    WriteBuf::push_prefix(&mut bufs, pad.pack());
    shared.driver.writev_all(active.handle.file(), end, bufs)?;
    Ok(alloc_high)
}

/// What the sealer's worker is asked to do
pub(super) enum Job {
    /// Close a segment the tail rolled off, cutting it at the offset given
    Seal { retired: Active, end: u64 },

    /// Run a flush the async door owed, since a caller with no thread cannot
    Flush(Owed),
}

/// The thread that seals segments a tail has rolled off
///
/// One per tail, idle almost always: it wakes once per segment roll. What it takes off
/// the write path is a footer sort, a multi-megabyte write and an fsync, and it takes
/// the async door's owed flushes for the same reason. A flush still waits for it.
pub(super) struct Sealer {
    /// Everything a job needs, for the fallback with no worker to hand it to
    shared: Arc<ReelShared>,

    /// The tail's active segment, which is what a forwarded flush flushes
    active: Arc<RwLock<Active>>,

    /// Jobs the worker has yet to finish, counted since a job is done only at its footer
    unfinished: Arc<Tension<usize>>,

    /// Where a roll hands its segment over, dropped to stop the worker
    hand: Option<mpsc::Sender<Job>>,

    /// The worker itself, joined when the tail goes
    thread: Option<JoinHandle<()>>,
}

impl Sealer {
    pub(super) fn start(shared: Arc<ReelShared>, active: Arc<RwLock<Active>>) -> Sealer {
        let (hand, jobs) = mpsc::channel::<Job>();
        let unfinished = Arc::new(Tension::new(0usize));
        let counted = Arc::clone(&unfinished);
        let volume = Arc::clone(&shared);
        let head = Arc::clone(&active);
        let thread = std::thread::Builder::new()
            .name("reel-sealer".to_string())
            .spawn(move || {
                for job in jobs {
                    run_job(&volume, &head, job);
                    counted.slack_with(|count| *count = count.saturating_sub(1));
                }
            })
            .ok();
        Sealer {
            shared,
            active,
            unfinished,
            hand: Some(hand),
            thread,
        }
    }

    /// Hand a rolled segment over, or seal it here if there is no worker to take it
    pub(super) fn hand_over(&self, retired: Active, end: u64) -> Result<()> {
        self.send(Job::Seal { retired, end })
    }

    /// Hand an owed flush over, so no runtime worker is the one holding the fsync
    pub(super) fn forward(&self, owed: Owed) -> Result<()> {
        self.send(Job::Flush(owed))
    }

    pub(super) fn send(&self, job: Job) -> Result<()> {
        // Counted before it is sent, so a flush cannot see nothing unfinished while a
        // job is in the channel.
        self.unfinished.with(|count| *count += 1);
        let sent = match &self.hand {
            Some(hand) => hand.send(job).map_err(|held| held.0),
            None => Err(job),
        };
        let Err(job) = sent else {
            return Ok(());
        };

        // No thread, or a dead one, so the caller pays the work instead.
        self.unfinished
            .slack_with(|count| *count = count.saturating_sub(1));
        match job {
            Job::Seal { retired, end } => {
                let sealed = retire_segment(&self.shared, &retired, end);
                if sealed.is_err() {
                    park_broken_seal(&self.shared, retired, end);
                }
                sealed
            }
            Job::Flush(owed) => {
                run_forwarded(&self.shared, &self.active, &owed);
                Ok(())
            }
        }
    }

    /// Wait until every job handed over is finished
    pub(super) fn drain(&self) {
        self.unfinished.park(|count| (*count == 0).then_some(()));
    }

    /// The same wait for a caller with a worker to protect rather than a thread
    pub(super) async fn drain_wait(&self) {
        self.unfinished
            .wait(|count| (*count == 0).then_some(()))
            .await;
    }
}

/// Do one of the sealer's jobs
pub(super) fn run_job(shared: &Arc<ReelShared>, active: &RwLock<Active>, job: Job) {
    match job {
        Job::Seal { retired, end } => {
            if let Err(error) = retire_segment(shared, &retired, end) {
                // The segment stays unsealed, which recovery reads by walking it, and
                // parking it is what lets the maintenance tick finish the seal.
                tracing::warn!("a rolled segment did not seal: {error}");
                park_broken_seal(shared, retired, end);
            }
        }
        Job::Flush(owed) => run_forwarded(shared, active, &owed),
    }
}

/// Run a flush the async door handed over, and leave the answer on the segment
///
/// A failed flush leaves the segment marked terminal rather than rolled, and the next
/// writer through finds the mark and rolls.
pub(super) fn run_forwarded(shared: &Arc<ReelShared>, active: &RwLock<Active>, owed: &Owed) {
    let flushed = flush_active(shared, active, owed.segment);
    publish_flush(owed, &flushed);
    if let Err(error) = flushed {
        tracing::warn!("a forwarded flush of a reel segment failed: {error}");
        doom_active(active, owed.segment);
    }
}

/// Flush one segment, holding the tail only long enough to say what that covers
///
/// Nothing comes back when the tail has already left the segment, since the roll seals
/// it and the seal answers for its bytes.
pub(super) fn flush_active(
    shared: &Arc<ReelShared>,
    active: &RwLock<Active>,
    segment: SegmentId,
) -> Result<Option<u64>> {
    let (handle, covered) = {
        let active = write(active);
        if active.handle.id() != segment || active.terminal.load(Ordering::Acquire) {
            return Ok(None);
        }
        // Taking the segment exclusively is what makes the byte count mean something:
        // every writer holds it shared across its whole reservation, so what has settled
        // while it is held is what has landed.
        (
            active.handle.clone(),
            active.settled.load(Ordering::Acquire),
        )
    };

    shared.driver.sync_data(handle.file())?;
    Ok(Some(covered))
}

/// Say what one flush did to the segment's durability, and wake whoever waited
pub(super) fn publish_flush(owed: &Owed, flushed: &Result<Option<u64>>) {
    match flushed {
        Ok(Some(covered)) => owed.sync.flush.slack_with(|flush| {
            // Two flushes of one segment can finish out of order, and the one answering
            // for less must not walk the watermark back.
            owed.sync.synced_at.fetch_max(*covered, Ordering::AcqRel);
            flush.is_running = false;
        }),
        Ok(None) => owed.sync.flush.slack_with(|flush| {
            flush.is_running = false;
            flush.is_retired = true;
        }),
        Err(_) => owed.sync.flush.slack_with(|flush| {
            flush.is_running = false;
            flush.is_broken = true;
        }),
    }
}

/// Mark a segment unwritable if the tail is still on it, so the next writer rolls
pub(super) fn doom_active(active: &RwLock<Active>, segment: SegmentId) -> bool {
    let active = read(active);
    if active.handle.id() != segment {
        return false;
    }
    active.terminal.store(true, Ordering::Release);
    true
}

impl Drop for Sealer {
    /// Stop the worker and wait for it, so a volume outlives its last seal
    fn drop(&mut self) {
        // Dropping the sender is what ends the worker's loop once it has drained.
        self.hand = None;
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}
