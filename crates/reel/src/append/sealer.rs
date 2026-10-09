//! The per-tail worker that seals rolled segments and runs owed flushes

use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::sync::{Arc, RwLock};
use std::thread::JoinHandle;

use crate::config::RepairPath;
use crate::error::{ReelError, Result};
use crate::format::footer::{seal_mark, SegmentFooter};
use crate::format::loc::SegmentId;
use crate::io::op::{Advice, Part, WriteBuf};
use crate::reel::ReelShared;
use crate::sync::tension::Tension;
use crate::sync::{lock, read, write};

use super::flush::Owed;
use super::Active;

/// Seal a segment, which closes it and leaves it readable and never written again
pub(super) fn seal_segment(shared: &Arc<ReelShared>, active: &Active, end: u64) -> Result<()> {
    if active.terminal.load(Ordering::Acquire) {
        return Ok(());
    }
    // With no peers to repair from, sync the records before the footer goes down
    if shared.config.repair == RepairPath::None {
        shared.driver.sync_full(active.handle.file())?;
    }

    let mut footer = std::mem::replace(&mut *lock(&active.entries), SegmentFooter::empty());
    // The segment's live and dead tally, which a rebuild cannot work out cheaply
    footer.tally = shared.tally_of(active.handle.id());
    // The tally is current up to this sequence number
    footer.sealed_at = shared.lsn.peek();
    let written = (|| -> Result<()> {
        let (rows, tail) = footer.pack_apart(shared.config.filter_bits)?;
        // Write the rows straight from their partitions and put them back after
        let rows: Vec<Arc<Vec<u8>>> = rows.into_iter().map(Arc::new).collect();
        let footer_len = rows.iter().map(|piece| piece.len()).sum::<usize>() + tail.len();
        let sealed_end = end + footer_len as u64;
        let mut pieces: Vec<WriteBuf> = rows
            .iter()
            .filter(|piece| !piece.is_empty())
            .map(|piece| WriteBuf::Part(Part::new(piece, 0, piece.len())))
            .collect();
        pieces.push(WriteBuf::owned(tail));
        // Keep the rows until the footer that lists them is on disk
        let rows_at = active.journal.rows_at();
        let wrote = match sealed_end < rows_at {
            true => shared.driver.writev_all(active.handle.file(), end, pieces),
            false => Err(ReelError::Corruption(format!(
                "a footer ending at {sealed_end} reaches the rows at {rows_at}"
            ))),
        };
        footer.put_rows(
            rows.into_iter()
                .map(|piece| Arc::try_unwrap(piece).unwrap_or_else(|piece| Vec::clone(&piece)))
                .collect(),
        );
        wrote?;
        active
            .journal
            .mark_sealed(&seal_mark(sealed_end, footer_len as u32))?;
        shared.driver.sync_full(active.handle.file())?;
        // Cut the rows so the footer ends the file, and a lost cut leaves the mark for reopen
        shared.driver.truncate(active.handle.file(), sealed_end)?;
        shared.driver.sync_full(active.handle.file())
    })();
    if let Err(error) = written {
        // A failed seal puts the footer back for the maintenance tick to retry
        *lock(&active.entries) = footer;
        return Err(error);
    }
    // The packed footer goes to the index as is, so the index never reads it back
    let footer = Arc::new(footer);
    // The handle now serves readers from the cache, so give it the read-path hint
    let _ = shared
        .driver
        .advise(active.handle.file(), 0, 0, Advice::Random);
    shared.fd_cache.insert(active.handle.clone());
    // The synced footer lists every record the journal did, so the journal can go
    active.journal.remove();
    shared.forget_unsealed(active.handle.id());
    // The footer is on disk, so a paged index can take this segment's keys from the map
    shared.note_sealed(active.handle.id(), footer);
    Ok(())
}

/// Seal a retired segment and say what it did to its durability
pub(super) fn retire_segment(shared: &Arc<ReelShared>, retired: &Active, end: u64) -> Result<()> {
    let sealed = seal_segment(shared, retired, end);
    let is_terminal = retired.terminal.load(Ordering::Acquire);
    match &sealed {
        // A terminal segment skipped its seal, so its broken mark stays
        Ok(()) if is_terminal => {}
        Ok(()) => retired.sync.mark_durable(),
        Err(_) => retired.sync.mark_broken(),
    }
    // Count a segment that broke with bytes unsynced, so later flushes stop answering clean
    if (sealed.is_err() || is_terminal) && retired.sync.covered() < end {
        shared.note_past_saving();
    }
    // No footer went down, so mark it unsealed before the release takes the holds off
    if sealed.is_err() || is_terminal {
        shared.note_unsealed(retired.handle.id());
    }
    // A failed seal keeps the segment unreleased, so compaction cannot retire it while it waits
    if sealed.is_ok() || is_terminal {
        shared.release_segment(retired.handle.id());
    }
    sealed
}

/// A rolled segment whose seal failed, parked for the maintenance tick
pub(crate) struct BrokenSeal {
    active: Active,
    end: u64,

    counted: bool,
}

/// Park a segment whose seal failed, unless its tail is closing
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
pub(crate) fn retry_broken_seals(shared: &Arc<ReelShared>) -> usize {
    let parked = std::mem::take(&mut *lock(&shared.broken_seals));
    if parked.is_empty() {
        return 0;
    }
    let mut sealed = 0usize;
    for job in parked {
        // A segment that turned terminal while parked will never seal, so release it
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

/// What the sealer's worker is asked to do
pub(super) enum Job {
    /// Close a segment the tail rolled off, cutting it at the offset given
    Seal { retired: Active, end: u64 },

    /// Run a flush the async door owed, since a caller with no thread cannot
    Flush(Owed),
}

/// The per-tail thread that seals rolled-off segments and runs forwarded flushes
pub(super) struct Sealer {
    /// Everything a job needs, for the fallback with no worker to hand it to
    shared: Arc<ReelShared>,

    /// The tail's active segment, which is what a forwarded flush flushes
    active: Arc<RwLock<Active>>,

    /// The count of jobs the worker has not finished
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

    /// Hand an owed flush over, so no runtime worker blocks on the fsync
    pub(super) fn forward(&self, owed: Owed) -> Result<()> {
        self.send(Job::Flush(owed))
    }

    pub(super) fn send(&self, job: Job) -> Result<()> {
        // Count before sending, so a flush never sees zero while a job is in the channel
        self.unfinished.with(|count| *count += 1);
        let sent = match &self.hand {
            Some(hand) => hand.send(job).map_err(|held| held.0),
            None => Err(job),
        };
        let Err(job) = sent else {
            return Ok(());
        };

        // No thread, or a dead one, so the caller does the work
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

    /// The same wait, awaited
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
                // Recovery can walk the unsealed segment, and the tick finishes the parked seal
                tracing::warn!("a rolled segment did not seal: {error}");
                park_broken_seal(shared, retired, end);
            }
        }
        Job::Flush(owed) => run_forwarded(shared, active, &owed),
    }
}

/// Run a flush the async door handed over, and leave the answer on the segment
pub(super) fn run_forwarded(shared: &Arc<ReelShared>, active: &RwLock<Active>, owed: &Owed) {
    let flushed = flush_active(shared, active, owed.segment);
    publish_flush(owed, &flushed);
    if let Err(error) = flushed {
        tracing::warn!("a forwarded flush of a reel segment failed: {error}");
        doom_active(active, owed.segment);
    }
}

/// Flush one segment, holding the tail only long enough to read what that covers
pub(super) fn flush_active(
    shared: &Arc<ReelShared>,
    active: &RwLock<Active>,
    segment: SegmentId,
) -> Result<Option<u64>> {
    let (handle, journal, covered) = {
        let active = write(active);
        if active.handle.id() != segment || active.terminal.load(Ordering::Acquire) {
            return Ok(None);
        }
        // Writers hold the segment shared, so under this hold everything settled has landed
        (
            active.handle.clone(),
            Arc::clone(&active.journal),
            active.settled.load(Ordering::Acquire),
        )
    };

    // Write pending rows first, so this one sync covers rows and records
    journal.write_pending()?;
    shared.driver.sync_data(handle.file())?;
    Ok(Some(covered))
}

/// Say what one flush did to the segment's durability, and wake whoever waited
pub(super) fn publish_flush(owed: &Owed, flushed: &Result<Option<u64>>) {
    match flushed {
        Ok(Some(covered)) => owed.sync.flush.slack_with(|flush| {
            // Flushes can finish out of order, so never move the watermark back
            let before = owed.sync.synced_at.fetch_max(*covered, Ordering::AcqRel);
            // The bytes written since the last flush, zero for one that finished out of order
            let span = covered.saturating_sub(before);
            if span > 0 {
                owed.sync.last_span.store(span, Ordering::Release);
            }
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
        // Dropping the sender ends the worker's loop once it drains
        self.hand = None;
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}
