//! Refcounted segment handles, unlink on last drop, and the bounded fd cache
//!
//! A segment file is reached through a refcounted handle so it is never unlinked
//! while a reader holds a reference; the last handle to drop performs the unlink.
//! Sealed handles are kept in a bounded cache, and all handle io runs through a
//! shared driver that hands each caller its own completions back through the slot
//! a tag addresses.

use std::cell::RefCell;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock, RwLock};

use crate::error::{ReelError, Result};
use crate::format::loc::SegmentId;
use crate::io::mapping::Mapping;
use crate::io::op::{
    Advice, ColdRoute, Completion, FileId, Op, Outcome, ReadBuf, SegmentEntry, Tag, WarmFirst,
    WriteBuf,
};
use crate::io::slots::{runs_of, SlotTable};
use crate::io::ReelIo;
use crate::sync::{read, write};

/// What one read of a batch filled, or why that read alone could not be served
///
/// The header and key come back apart from the payload, which is what lets a
/// caller keep the payload buffer it was read into rather than shifting it down.
pub type SplitRead = std::result::Result<(Vec<u8>, Vec<u8>), ReelError>;

/// What one split read filled, or its error and the spare buffer it came in with
///
/// The spare comes back on a failure, so a read that did not land leaves the
/// caller's own header buffer with the caller rather than with the allocator.
pub type SplitAnswer = std::result::Result<(Vec<u8>, Vec<u8>), (ReelError, Vec<u8>)>;

/// How many empty poll rounds the driver waits for a completion before giving up
const MAX_POLL_ROUNDS: u32 = 1_000_000;

/// Empty poll rounds the driver spins before it starts yielding the core
const SPIN_ROUNDS: u32 = 64;

/// Correlates concurrent submitters with one backend's shared completion queue
///
/// Submit takes no driver-wide lock, so the synchronous backends run their syscalls
/// on the caller threads. One thread's drain can surface another thread's
/// completion, so every drain is filed into the slot its tag addresses and each
/// caller takes only its own.
pub struct IoDriver {
    /// The backend every op goes down to
    backend: Arc<dyn ReelIo>,

    /// The slots every op lands in, shared so a backend's own threads can file
    slots: Arc<SlotTable>,
}

impl IoDriver {
    /// A driver over one backend
    pub fn new(backend: Arc<dyn ReelIo>) -> IoDriver {
        IoDriver {
            backend,
            slots: Arc::new(SlotTable::new()),
        }
    }

    /// Device flushes the backend under this driver has asked for
    pub fn sync_count(&self) -> u64 {
        self.backend.sync_count()
    }

    /// Which door the backend's ops took, ring or the fallback beside it
    pub fn door_counts(&self) -> crate::io::DoorCounts {
        self.backend.door_counts()
    }

    /// Nanoseconds the backend spent waiting inside those flushes
    pub fn sync_nanos(&self) -> u64 {
        self.backend.sync_nanos()
    }

    /// A fresh tag unique for the life of this driver
    pub fn next_tag(&self) -> Tag {
        self.slots.next_tag()
    }

    /// How many ops this driver keeps in flight at once
    pub fn slots(&self) -> usize {
        self.slots.slots()
    }

    /// How many ops one awaited batch asks for at a time
    ///
    /// A batch future claims its whole run or none of it, so a quarter of the table
    /// leaves room for three more callers.
    pub fn batch_slots(&self) -> usize {
        self.slots.slots() / 4
    }

    /// Flights claimed and not yet answered
    pub fn outstanding(&self) -> usize {
        self.slots.outstanding()
    }

    /// Slots holding a waker, which is the futures currently pending
    pub fn wakers(&self) -> usize {
        self.slots.wakers()
    }

    /// Completions put back rather than delivered, which is what a drop costs
    pub fn reclaimed(&self) -> u64 {
        self.slots.reclaimed()
    }

    /// Drain what the backend has ready into the slots waiting for it
    ///
    /// A thread already draining is left to it, and that turn is answered as nothing
    /// moved.
    pub fn reap(&self) -> Result<usize> {
        let Some(mut drained) = self.slots.begin_poll() else {
            return Ok(0);
        };
        let filed = self.backend.poll(&mut drained);
        self.slots.end_poll(drained);
        filed
    }

    /// Submit a batch and collect exactly its completions, in submit order
    pub fn run(&self, ops: Vec<Op>) -> Result<Vec<Completion>> {
        let mut ops = ops;
        let mut out = Vec::with_capacity(ops.len());
        self.run_into(&mut ops, &mut out)?;
        Ok(out)
    }

    /// The same batch into vectors the caller keeps, taking the ops out of theirs
    ///
    /// The completions land in the caller's list and the op list comes back empty
    /// but with its room intact, so a thread batching reads keeps both between
    /// submissions.
    pub fn run_into(&self, ops: &mut Vec<Op>, out: &mut Vec<Completion>) -> Result<()> {
        out.clear();
        // A backend that runs the batch on this thread hands the completions back
        // in submit order, so the slots have nothing left to correlate.
        if self.backend.submit_batch(ops, out) {
            return Ok(());
        }

        let wanted: Vec<Tag> = ops.iter().map(|op| op.tag()).collect();
        let mut filled: Vec<Option<Completion>> = Vec::new();
        filled.resize_with(wanted.len(), || None);

        // A run is as much of the batch as the table has slots for, so a batch
        // wider than the table goes down in runs as the ones ahead of it free.
        let mut at = 0;
        while at < wanted.len() {
            let run = self.claim_run(&wanted[at..])?;
            let batch: Vec<Op> = ops.drain(..run).collect();
            if let Err(error) = self.backend.submit(batch) {
                self.slots.release_run(&wanted[at..at + run]);
                return Err(error);
            }
            self.collect(&wanted[at..at + run], &mut filled[at..at + run])?;
            at += run;
        }

        out.reserve(filled.len());
        for slot in filled {
            match slot {
                Some(completion) => out.push(completion),
                None => return Err(dropped_completion()),
            }
        }
        Ok(())
    }

    /// The slot table's filing ledger, for stall diagnosis
    pub fn debug_flights(&self) -> String {
        self.slots.debug_flights()
    }

    /// Submit one op and collect its completion without the batch bookkeeping
    ///
    /// A backend that services ops on the calling thread answers in place, which
    /// skips the slot and its lock.
    fn run_op(&self, op: Op) -> Result<Completion> {
        let op = match self.backend.submit_inline(op) {
            Ok(completion) => return Ok(completion),
            Err(op) => op,
        };
        let wanted = [op.tag()];
        self.claim_run(&wanted)?;
        if let Err(error) = self.backend.submit(vec![op]) {
            self.slots.release_run(&wanted);
            return Err(error);
        }
        let mut filled = [None];
        self.collect(&wanted, &mut filled)?;
        match filled[0].take() {
            Some(completion) => Ok(completion),
            None => Err(dropped_completion()),
        }
    }

    /// Submit one op and await its completion
    ///
    /// The claim is awaited rather than parked on, because the caller polling this
    /// is the caller holding the futures whose completions free the slots.
    pub async fn wait_op(&self, op: Op) -> Result<Completion> {
        let wanted = [op.tag()];
        self.slots.claim(&wanted).await;
        if let Err(error) = self.backend.submit_detached_one(op, &self.slots) {
            self.slots.release_run(&wanted);
            return Err(error);
        }
        // The awaited door serves itself: a poll that finds the slot empty drains
        // the backend if nobody else is. The waker is seated before that drain, so
        // a completion another thread files in the gap still wakes this future.
        let wait = self.slots.wait_op(wanted[0]);
        let mut wait = std::pin::pin!(wait);
        Ok(std::future::poll_fn(move |context| {
            match std::future::Future::poll(wait.as_mut(), context) {
                std::task::Poll::Ready(completion) => std::task::Poll::Ready(completion),
                std::task::Poll::Pending => {
                    if self.slots.drain_once(|scratch| self.backend.poll(scratch)) {
                        std::future::Future::poll(wait.as_mut(), context)
                    } else {
                        std::task::Poll::Pending
                    }
                }
            }
        })
        .await)
    }

    /// Submit a batch and await its completions, in submit order
    ///
    /// The whole batch is claimed at once or not at all, so two batches cannot each
    /// hold half of what they need and wait on the other for the rest.
    pub async fn wait_batch(&self, ops: Vec<Op>) -> Result<Vec<Completion>> {
        if ops.len() > self.batch_slots() {
            return Err(ReelError::Rejected(format!(
                "a batch of {} ops is past the driver's {} awaited slots",
                ops.len(),
                self.batch_slots()
            )));
        }

        let wanted: Vec<Tag> = ops.iter().map(|op| op.tag()).collect();
        self.slots.claim(&wanted).await;
        if let Err(error) = self.backend.submit_detached(ops, &self.slots) {
            self.slots.release_run(&wanted);
            return Err(error);
        }
        // The same self-service the single op takes: a pending batch drains the
        // backend when nobody else is.
        let wait = self.slots.wait_batch(runs_of(&wanted), wanted.len());
        let mut wait = std::pin::pin!(wait);
        Ok(std::future::poll_fn(move |context| {
            match std::future::Future::poll(wait.as_mut(), context) {
                std::task::Poll::Ready(out) => std::task::Poll::Ready(out),
                std::task::Poll::Pending => {
                    if self.slots.drain_once(|scratch| self.backend.poll(scratch)) {
                        std::future::Future::poll(wait.as_mut(), context)
                    } else {
                        std::task::Poll::Pending
                    }
                }
            }
        })
        .await)
    }

    /// Claim slots for as much of a batch as the table has room for right now
    ///
    /// At least one, and a slot comes back only when whoever holds it takes its
    /// completion, so this waits on the table rather than on the backend.
    fn claim_run(&self, wanted: &[Tag]) -> Result<usize> {
        self.slots.wait_free(
            || {
                let run = self.slots.claim_run(wanted);
                (run > 0).then_some(run)
            },
            |scratch| self.backend.poll(scratch),
        )
    }

    /// Wait until every wanted tag has landed a completion in its slot
    ///
    /// A wait that gives up orphans the flights it never saw rather than freeing
    /// their slots, so a late completion is reclaimed instead of filed against
    /// whoever holds the slot by then.
    fn collect(&self, wanted: &[Tag], slots: &mut [Option<Completion>]) -> Result<()> {
        let mut rounds = 0u32;
        let mut missing = slots.len();
        let taken = self.slots.pump(
            || {
                for (slot, tag) in slots.iter_mut().zip(wanted) {
                    if slot.is_some() {
                        continue;
                    }
                    if let Some(completion) = self.slots.take(*tag) {
                        *slot = Some(completion);
                        missing -= 1;
                    }
                }
                (missing == 0).then_some(())
            },
            |scratch| self.poll_until_drained(scratch, &mut rounds),
        );

        if let Err(error) = taken {
            for (slot, tag) in slots.iter().zip(wanted) {
                if slot.is_none() {
                    self.slots.orphan(*tag);
                }
            }
            return Err(error);
        }
        Ok(())
    }

    /// Poll the backend until it drains something or the round budget runs out
    ///
    /// The first rounds spin, since a completion already in flight lands within a
    /// few of them. Past that a backend that can park is asked to, since a spinning
    /// thread holds a core to find out.
    fn poll_until_drained(&self, scratch: &mut Vec<Completion>, rounds: &mut u32) -> Result<usize> {
        loop {
            let drained = self.backend.poll(scratch)?;
            if drained > 0 {
                return Ok(drained);
            }
            *rounds += 1;
            if *rounds >= MAX_POLL_ROUNDS {
                return Ok(0);
            }
            if *rounds > SPIN_ROUNDS {
                if self.backend.parks_on_wait() {
                    let drained = self.backend.poll_blocking(scratch)?;
                    if drained > 0 {
                        return Ok(drained);
                    }
                } else {
                    std::thread::yield_now();
                }
            }
        }
    }

    /// Open or create a file, yielding the handle later ops name
    pub fn open(&self, path: &Path, create: bool) -> Result<FileId> {
        let op = Op::Open {
            tag: self.next_tag(),
            path: path.to_path_buf(),
            create,
            direct: false,
        };
        match self.run_op(op)?.outcome {
            Outcome::Opened(result) => result,
            other => Err(wrong_shape(&other)),
        }
    }

    /// Open a second descriptor on a file whose reads skip the page cache
    ///
    /// Never creates: this is another view of a file the buffered open already made.
    pub fn open_direct(&self, path: &Path) -> Result<FileId> {
        let op = Op::Open {
            tag: self.next_tag(),
            path: path.to_path_buf(),
            create: false,
            direct: true,
        };
        match self.run_op(op)?.outcome {
            Outcome::Opened(result) => result,
            other => Err(wrong_shape(&other)),
        }
    }

    /// Read a byte range, returning only the bytes the backend filled
    pub fn pread(&self, file: FileId, offset: u64, len: u64) -> Result<Vec<u8>> {
        self.pread_reusing(file, offset, len, Vec::new())
    }

    /// Read a byte range into a buffer the caller is done with
    ///
    /// The buffer comes back holding what the read filled, so a caller stepping a
    /// file keeps one allocation for the whole walk instead of one per read.
    pub fn pread_reusing(
        &self,
        file: FileId,
        offset: u64,
        len: u64,
        reuse: Vec<u8>,
    ) -> Result<Vec<u8>> {
        if len == 0 {
            return Ok(Vec::new());
        }
        let op = Op::Pread {
            tag: self.next_tag(),
            file,
            offset,
            buf: ReadBuf::reusing(reuse, len as usize),
        };
        match self.run_op(op)?.outcome {
            Outcome::Read { result, buf } => {
                result?;
                Ok(buf.into_vec())
            }
            other => Err(wrong_shape(&other)),
        }
    }

    /// Read a byte range as a future, into a buffer the caller is done with
    pub async fn wait_pread_reusing(
        &self,
        file: FileId,
        offset: u64,
        len: u64,
        reuse: Vec<u8>,
    ) -> Result<Vec<u8>> {
        if len == 0 {
            return Ok(Vec::new());
        }
        let op = Op::Pread {
            tag: self.next_tag(),
            file,
            offset,
            buf: ReadBuf::reusing(reuse, len as usize),
        };
        match self.wait_op(op).await?.outcome {
            Outcome::Read { result, buf } => {
                result?;
                Ok(buf.into_vec())
            }
            other => Err(wrong_shape(&other)),
        }
    }

    /// Read a window through the plane its route names, into a reused buffer
    ///
    /// One op whatever the route: the plane is chosen inside the backend, so a
    /// window is one slot and one completion either way.
    pub fn pread_cold(
        &self,
        file: FileId,
        route: ColdRoute,
        offset: u64,
        len: u64,
        reuse: Vec<u8>,
    ) -> Result<Vec<u8>> {
        if len == 0 {
            return Ok(Vec::new());
        }
        let op = cold_read_op(self.next_tag(), file, route, offset, len, reuse);
        match self.run_op(op)?.outcome {
            Outcome::Read { result, buf } => {
                result?;
                Ok(buf.into_vec())
            }
            other => Err(wrong_shape(&other)),
        }
    }

    /// The same routed window as a future, awaited rather than parked on
    pub async fn wait_pread_cold(
        &self,
        file: FileId,
        route: ColdRoute,
        offset: u64,
        len: u64,
        reuse: Vec<u8>,
    ) -> Result<Vec<u8>> {
        if len == 0 {
            return Ok(Vec::new());
        }
        let op = cold_read_op(self.next_tag(), file, route, offset, len, reuse);
        match self.wait_op(op).await?.outcome {
            Outcome::Read { result, buf } => {
                result?;
                Ok(buf.into_vec())
            }
            other => Err(wrong_shape(&other)),
        }
    }

    /// Read one contiguous range split across a header buffer and a payload buffer
    ///
    /// The header buffer is the caller's spare and comes back either way, including
    /// alongside the error when the read fails. Only a failed submission loses it.
    /// Read one split range, parking the caller for the completion
    ///
    /// `WarmFirst::Ask` puts one non-blocking read ahead of the op, the same way
    /// the awaited twin does.
    pub fn pread_split_reusing(
        &self,
        file: FileId,
        offset: u64,
        head_len: usize,
        body_len: usize,
        spare: Vec<u8>,
        warm: WarmFirst,
    ) -> SplitAnswer {
        let mut head = ReadBuf::reusing(spare, head_len);
        let mut body = ReadBuf::new(body_len);
        if warm == WarmFirst::Ask && self.backend.warm_split(file, offset, &mut head, &mut body) {
            return Ok((head.into_vec(), body.into_vec()));
        }
        let op = Op::PreadSplit {
            tag: self.next_tag(),
            file,
            offset,
            head,
            body,
        };
        let completion = match self.run_op(op) {
            Ok(completion) => completion,
            Err(error) => return Err((error, Vec::new())),
        };
        split_answer(completion)
    }

    /// Read one split range as a future rather than by parking the caller
    ///
    /// WarmFirst::Ask puts one non-blocking read ahead of the op, so a record the
    /// page cache already holds is answered with no tag, slot or completion spent.
    pub async fn wait_split_reusing(
        &self,
        file: FileId,
        offset: u64,
        head_len: usize,
        body_len: usize,
        spare: Vec<u8>,
        warm: WarmFirst,
    ) -> SplitAnswer {
        let mut head = ReadBuf::reusing(spare, head_len);
        let mut body = ReadBuf::new(body_len);
        if warm == WarmFirst::Ask && self.backend.warm_split(file, offset, &mut head, &mut body) {
            return Ok((head.into_vec(), body.into_vec()));
        }
        let op = Op::PreadSplit {
            tag: self.next_tag(),
            file,
            offset,
            head,
            body,
        };
        let completion = match self.wait_op(op).await {
            Ok(completion) => completion,
            Err(error) => return Err((error, Vec::new())),
        };
        split_answer(completion)
    }

    /// Build a split read without submitting it, for a caller assembling a batch
    pub fn split_read(&self, file: FileId, offset: u64, head_len: usize, body_len: usize) -> Op {
        Op::PreadSplit {
            tag: self.next_tag(),
            file,
            offset,
            head: ReadBuf::new(head_len),
            body: ReadBuf::new(body_len),
        }
    }

    /// Run a batch of split reads and hand back what each of them filled
    ///
    /// One read failing comes back in its own slot; the batch failing comes back as
    /// the error, so a caller can tell a record it cannot have from a device it
    /// cannot reach.
    pub fn run_split_reads(&self, ops: Vec<Op>) -> Result<Vec<SplitRead>> {
        let mut ops = ops;
        let mut completions = Vec::new();
        let mut filled = Vec::with_capacity(ops.len());
        self.run_split_reads_into(&mut ops, &mut completions, &mut filled)?;
        Ok(filled)
    }

    /// The same batch through vectors the caller keeps between submissions
    pub fn run_split_reads_into(
        &self,
        ops: &mut Vec<Op>,
        completions: &mut Vec<Completion>,
        filled: &mut Vec<SplitRead>,
    ) -> Result<()> {
        filled.clear();
        self.run_into(ops, completions)?;
        collect_split_reads(completions.drain(..), filled)
    }

    /// Run a batch of split reads as a future, in runs the slot table can hold
    ///
    /// One caller may not hold the whole table, so a batch wider than its share is
    /// sent down as several and each awaited in turn.
    pub async fn wait_split_reads(&self, mut ops: Vec<Op>) -> Result<Vec<SplitRead>> {
        let mut filled = Vec::with_capacity(ops.len());
        self.wait_split_reads_into(&mut ops, &mut filled).await?;
        Ok(filled)
    }

    /// The same awaited batch, taking the ops out of a vector the caller keeps
    pub async fn wait_split_reads_into(
        &self,
        ops: &mut Vec<Op>,
        filled: &mut Vec<SplitRead>,
    ) -> Result<()> {
        filled.clear();
        while !ops.is_empty() {
            let run = ops.len().min(self.batch_slots());
            let batch: Vec<Op> = ops.drain(..run).collect();
            collect_split_reads(self.wait_batch(batch).await?, filled)?;
        }
        Ok(())
    }

    /// Append owned buffers at an offset in one vectored write
    pub fn writev(&self, file: FileId, offset: u64, bufs: Vec<WriteBuf>) -> Result<u64> {
        let op = Op::Writev {
            tag: self.next_tag(),
            file,
            offset,
            bufs,
        };
        match self.run_op(op)?.outcome {
            Outcome::Wrote { result, .. } => result,
            other => Err(wrong_shape(&other)),
        }
    }

    /// The same write, refused where it landed short of what it framed
    ///
    /// A vectored write answers with a count, and every caller here is placing bytes
    /// something else is measured against, so short is a fault rather than a number
    /// to carry.
    pub fn writev_all(&self, file: FileId, offset: u64, bufs: Vec<WriteBuf>) -> Result<()> {
        let framed: u64 = bufs.iter().map(|buf| buf.len() as u64).sum();
        let wrote = self.writev(file, offset, bufs)?;
        if wrote != framed {
            return Err(ReelError::Io(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                format!("a write framed {framed} bytes and landed {wrote}"),
            )));
        }
        Ok(())
    }

    /// The same write, handing the buffers back so a caller reusing one keeps its room
    ///
    /// A write takes its buffers by value, since the device holds their addresses
    /// until the completion.
    pub fn writev_reusing(
        &self,
        file: FileId,
        offset: u64,
        bufs: Vec<WriteBuf>,
    ) -> Result<(u64, Vec<WriteBuf>)> {
        let op = Op::Writev {
            tag: self.next_tag(),
            file,
            offset,
            bufs,
        };
        match self.run_op(op)?.outcome {
            Outcome::Wrote { result, bufs } => Ok((result?, bufs)),
            other => Err(wrong_shape(&other)),
        }
    }

    /// List a directory, reporting one that does not exist yet as empty
    ///
    /// Only absence is an empty listing: answering an unreadable directory that way
    /// would let an open conclude the volume holds nothing.
    pub fn list_or_empty(&self, dir: &Path) -> Result<Vec<SegmentEntry>> {
        match self.list(dir) {
            Ok(entries) => Ok(entries),
            Err(error) if error.is_missing() => Ok(Vec::new()),
            Err(error) => Err(error),
        }
    }

    /// Length of one open file, without walking the directory that holds it
    pub fn length(&self, file: FileId) -> Result<u64> {
        let op = Op::Length {
            tag: self.next_tag(),
            file,
        };
        match self.run_op(op)?.outcome {
            Outcome::Length(result) => result,
            other => Err(wrong_shape(&other)),
        }
    }

    /// Flush a file's data, the cadence sync
    pub fn sync_data(&self, file: FileId) -> Result<()> {
        self.done(Op::SyncData {
            tag: self.next_tag(),
            file,
        })
    }

    /// Flush a file fully, at seal and before an unlink
    pub fn sync_full(&self, file: FileId) -> Result<()> {
        self.done(Op::SyncFull {
            tag: self.next_tag(),
            file,
        })
    }

    /// Flush a directory so a create, rename, or unlink is durable
    pub fn sync_dir(&self, dir: &Path) -> Result<()> {
        self.done(Op::SyncDir {
            tag: self.next_tag(),
            dir: dir.to_path_buf(),
        })
    }

    /// Tell the kernel how a range will be read, or that a range is no longer wanted
    ///
    /// A zero length names the whole file, which is how a descriptor-wide access
    /// pattern is set.
    pub fn advise(&self, file: FileId, offset: u64, len: u64, advice: Advice) -> Result<()> {
        self.done(Op::Advise {
            tag: self.next_tag(),
            file,
            offset,
            len,
            advice,
        })
    }

    /// Release a file handle, freeing the descriptor behind it
    pub fn close(&self, file: FileId) -> Result<()> {
        self.done(Op::Close {
            tag: self.next_tag(),
            file,
        })
    }

    /// Move a path, the step that records a reel as retired in one atomic stroke
    pub fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        self.done(Op::Rename {
            tag: self.next_tag(),
            from: from.to_path_buf(),
            to: to.to_path_buf(),
        })
    }

    /// Remove a file
    pub fn unlink(&self, path: &Path) -> Result<()> {
        self.done(Op::Unlink {
            tag: self.next_tag(),
            path: path.to_path_buf(),
        })
    }

    /// List the segment files under a directory
    pub fn list(&self, dir: &Path) -> Result<Vec<SegmentEntry>> {
        let op = Op::List {
            tag: self.next_tag(),
            dir: dir.to_path_buf(),
        };
        match self.run_op(op)?.outcome {
            Outcome::Listed(result) => result,
            other => Err(wrong_shape(&other)),
        }
    }

    /// Reserve space ahead of the write head, extending the file to cover it
    ///
    /// The reservation is part of the file's length, so a walk to the end of a
    /// preallocated segment runs into zeros rather than the last record written.
    pub fn allocate(&self, file: FileId, offset: u64, len: u64) -> Result<()> {
        self.done(Op::Allocate {
            tag: self.next_tag(),
            file,
            offset,
            len,
        })
    }

    /// Run one op whose completion carries no payload
    fn done(&self, op: Op) -> Result<()> {
        match self.run_op(op)?.outcome {
            Outcome::Done(result) => result,
            other => Err(wrong_shape(&other)),
        }
    }
}

/// The read one window takes, which is the plain one wherever it is not routed
fn cold_read_op(
    tag: Tag,
    file: FileId,
    route: ColdRoute,
    offset: u64,
    len: u64,
    reuse: Vec<u8>,
) -> Op {
    let buf = ReadBuf::reusing(reuse, len as usize);
    match route {
        ColdRoute::Cached => Op::Pread {
            tag,
            file,
            offset,
            buf,
        },
        ColdRoute::Probed(direct) => Op::PreadCold {
            tag,
            file,
            direct,
            offset,
            buf,
            probe: true,
        },
        ColdRoute::Direct(direct) => Op::PreadCold {
            tag,
            file,
            direct,
            offset,
            buf,
            probe: false,
        },
    }
}

/// What a split read's completion filled, or its error and the spare it carried
///
/// A read that did not land leaves the payload buffer the pool's and the header
/// buffer the caller's spare, so each goes back where it came from.
fn split_answer(completion: Completion) -> SplitAnswer {
    match completion.outcome {
        Outcome::ReadSplit { result, head, body } => match result {
            Ok(_) => Ok((head.into_vec(), body.into_vec())),
            Err(error) => {
                crate::reel::payload::give(body.into_vec());
                Err((error, head.into_vec()))
            }
        },
        other => Err((wrong_shape(&other), Vec::new())),
    }
}

/// File a batch's completions as split reads, one answer each in submit order
fn collect_split_reads(
    completions: impl IntoIterator<Item = Completion>,
    filled: &mut Vec<SplitRead>,
) -> Result<()> {
    for completion in completions {
        match completion.outcome {
            Outcome::ReadSplit { result, head, body } => {
                filled.push(result.map(|_| (head.into_vec(), body.into_vec())));
            }
            other => return Err(wrong_shape(&other)),
        }
    }
    Ok(())
}

/// The error a batch missing one of its completions maps to
fn dropped_completion() -> ReelError {
    ReelError::Backend("backend dropped a submitted completion".to_string())
}

/// The error a completion of the wrong shape maps to
pub(crate) fn wrong_shape(outcome: &Outcome) -> ReelError {
    ReelError::Backend(format!(
        "backend returned the wrong completion shape: {}",
        outcome_label(outcome)
    ))
}

fn outcome_label(outcome: &Outcome) -> &'static str {
    match outcome {
        Outcome::Opened(_) => "opened",
        Outcome::Wrote { .. } => "wrote",
        Outcome::Read { .. } => "read",
        Outcome::ReadSplit { .. } => "read split",
        Outcome::Listed(_) => "listed",
        Outcome::Length(_) => "length",
        Outcome::Done(_) => "done",
    }
}

/// Bytes a segment reader buys per trip to the backend
pub(crate) const READ_CHUNK: usize = 1 << 20;

/// The handle a tail holds before it has opened anything, naming no real file
///
/// Backends number their handles from zero, so a stand-in cannot borrow a number
/// they might issue: releasing it would release someone else's live segment.
const NO_FILE: FileId = FileId(u64::MAX);

/// A forward reader over one segment that buys its bytes a chunk at a time
///
/// A record walk asks for a header and then the payload behind it, so a resident
/// window turns one read per record into one read per chunk. The window only ever
/// moves forward.
pub struct SegmentReader<'driver> {
    driver: &'driver IoDriver,
    file: FileId,
    limit: u64,
    window: Vec<u8>,
    window_at: u64,
    read_bytes: u64,
}

impl<'driver> SegmentReader<'driver> {
    /// A reader over the first bytes of one open segment
    pub fn new(driver: &'driver IoDriver, file: FileId, limit: u64) -> SegmentReader<'driver> {
        SegmentReader {
            driver,
            file,
            limit,
            window: Vec::new(),
            window_at: 0,
            read_bytes: 0,
        }
    }

    /// Byte the reader stops at
    pub fn limit(&self) -> u64 {
        self.limit
    }

    /// Bytes the reader has bought from the backend across every refill
    ///
    /// What the reads cost the device rather than what callers asked for, which is
    /// what the rate gate charges from.
    pub fn read_bytes(&self) -> u64 {
        self.read_bytes
    }

    /// Bytes at an offset, short only where the segment itself runs out
    pub fn range(&mut self, offset: u64, len: usize) -> Result<&[u8]> {
        if offset >= self.limit {
            return Ok(&[]);
        }
        let wanted = (len as u64).min(self.limit - offset) as usize;
        if !self.window_holds(offset, wanted) {
            self.refill(offset, wanted)?;
        }
        let start = (offset - self.window_at) as usize;
        let end = (start + wanted).min(self.window.len());
        Ok(&self.window[start..end])
    }

    fn window_holds(&self, offset: u64, len: usize) -> bool {
        if offset < self.window_at {
            return false;
        }
        let start = (offset - self.window_at) as usize;
        start + len <= self.window.len()
    }

    fn refill(&mut self, offset: u64, wanted: usize) -> Result<()> {
        let span = (wanted.max(READ_CHUNK) as u64).min(self.limit - offset);
        let reuse = std::mem::take(&mut self.window);
        self.window = self.driver.pread_reusing(self.file, offset, span, reuse)?;
        self.window_at = offset;
        self.read_bytes += self.window.len() as u64;
        Ok(())
    }

    /// Bytes several ranges hold, fetched as one batch of outstanding reads
    ///
    /// Buffers come back in ask order, short only where the segment runs out, and a
    /// range past the limit comes back empty without an op.
    pub fn read_ranges(
        &mut self,
        ranges: &[(u64, usize)],
        pool: &mut Vec<Vec<u8>>,
    ) -> Result<Vec<Vec<u8>>> {
        let mut asked = Vec::with_capacity(ranges.len());
        let mut ops = Vec::with_capacity(ranges.len());
        for (at, &(offset, len)) in ranges.iter().enumerate() {
            let wanted = (len as u64).min(self.limit.saturating_sub(offset)) as usize;
            if wanted == 0 {
                continue;
            }
            asked.push(at);
            ops.push(Op::Pread {
                tag: self.driver.next_tag(),
                file: self.file,
                offset,
                buf: ReadBuf::reusing(pool.pop().unwrap_or_default(), wanted),
            });
        }
        let completions = self.driver.run(ops)?;
        let mut buffers = vec![Vec::new(); ranges.len()];
        for (at, completion) in asked.into_iter().zip(completions) {
            match completion.outcome {
                Outcome::Read { result, buf } => {
                    result?;
                    let bytes = buf.into_vec();
                    self.read_bytes += bytes.len() as u64;
                    buffers[at] = bytes;
                }
                other => return Err(wrong_shape(&other)),
            }
        }
        Ok(buffers)
    }

    /// Adopt a fetched range as the window, handing back the one it replaces
    pub fn preload(&mut self, offset: u64, window: Vec<u8>) -> Vec<u8> {
        self.window_at = offset;
        std::mem::replace(&mut self.window, window)
    }
}

/// What one segment's single direct open settled on
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DirectOpen {
    /// The descriptor is open and this segment's windows read the device
    Ready(FileId),

    /// This file has no direct descriptor, for a reason the next file need not share
    Refused,

    /// The filesystem serves no direct opens, so no file on it will have one
    Unsupported,
}

struct SegmentInner {
    id: SegmentId,
    path: PathBuf,
    file: FileId,
    driver: Arc<IoDriver>,
    is_doomed: AtomicBool,
    mapping: OnceLock<Option<Mapping>>,
    direct: OnceLock<DirectOpen>,
}

impl Drop for SegmentInner {
    /// Unlink a doomed segment, then release the descriptor either way
    ///
    /// The order is what reclaims the space: a filesystem frees a file's blocks only
    /// once its last link and its last descriptor are both gone.
    fn drop(&mut self) {
        if self.file == NO_FILE {
            return;
        }
        if self.is_doomed.load(Ordering::Acquire) {
            if let Err(error) = self.driver.unlink(&self.path) {
                tracing::warn!("failed to unlink a doomed reel segment on last drop: {error}");
            }
        }
        // Both descriptors, since either one left open holds the file's extents
        // where a directory walk can no longer see them.
        if let Some(DirectOpen::Ready(direct)) = self.direct.get() {
            if let Err(error) = self.driver.close(*direct) {
                tracing::warn!(
                    "failed to release a reel segment's direct descriptor on last drop: {error}"
                );
            }
        }
        if let Err(error) = self.driver.close(self.file) {
            tracing::warn!("failed to release a reel segment descriptor on last drop: {error}");
        }
    }
}

/// A refcounted reference to one open segment file
///
/// Cloning shares the underlying file; the file is unlinked only when the last
/// clone drops and the segment has been marked doomed.
#[derive(Clone)]
pub struct SegmentHandle {
    inner: Arc<SegmentInner>,
}

impl SegmentHandle {
    /// A handle to an open segment file served by a driver
    pub fn new(id: SegmentId, path: PathBuf, file: FileId, driver: Arc<IoDriver>) -> SegmentHandle {
        SegmentHandle {
            inner: Arc::new(SegmentInner {
                id,
                path,
                file,
                driver,
                is_doomed: AtomicBool::new(false),
                mapping: OnceLock::new(),
                direct: OnceLock::new(),
            }),
        }
    }

    /// A never-doomed stand-in a tail holds until it opens its first real segment
    pub fn placeholder(driver: Arc<IoDriver>) -> SegmentHandle {
        SegmentHandle::new(SegmentId(0), PathBuf::new(), NO_FILE, driver)
    }

    /// Segment number this handle refers to
    pub fn id(&self) -> SegmentId {
        self.inner.id
    }

    /// Open file handle backing this segment
    pub fn file(&self) -> FileId {
        self.inner.file
    }

    /// Path of the segment file
    pub fn path(&self) -> &Path {
        &self.inner.path
    }

    /// Mark the segment for unlink once the last handle drops
    pub fn mark_doomed(&self) {
        self.inner.is_doomed.store(true, Ordering::Release);
    }

    /// Whether the segment is marked for unlink
    pub fn is_doomed(&self) -> bool {
        self.inner.is_doomed.load(Ordering::Acquire)
    }

    /// Number of live references to this segment, including this handle
    pub fn reference_count(&self) -> usize {
        Arc::strong_count(&self.inner)
    }

    /// The segment's read-only mapping, taken on the first ask and kept for the
    /// life of the handle family
    ///
    /// A file that cannot be mapped stays unmapped for that life rather than buying
    /// a failed map per read.
    pub fn mapping(&self) -> Option<&Mapping> {
        self.inner
            .mapping
            .get_or_init(|| Mapping::open(&self.inner.path))
            .as_ref()
    }

    /// The segment's direct descriptor, if one has already been settled
    pub fn direct_file(&self) -> Option<FileId> {
        match self.inner.direct.get() {
            Some(DirectOpen::Ready(file)) => Some(*file),
            _ => None,
        }
    }

    /// The segment's direct descriptor, opening one on the first ask
    ///
    /// A file that cannot be opened this way stays buffered for the life of the
    /// handle family rather than buying a failed open per read. Whether the refusal
    /// is this file's or the whole filesystem's comes back with it, since only the
    /// second says anything about the segments not opened yet.
    pub fn direct_file_or_open(&self) -> DirectOpen {
        *self.inner.direct.get_or_init(|| {
            match self.inner.driver.open_direct(&self.inner.path) {
                Ok(file) => DirectOpen::Ready(file),
                Err(error) => {
                    tracing::warn!("a reel segment's windows read through the page cache: its direct open failed: {error}");
                    if error.is_unsupported() {
                        DirectOpen::Unsupported
                    } else {
                        DirectOpen::Refused
                    }
                }
            }
        })
    }
}

/// What names one segment file across a whole volume
///
/// A volume holds one reel and a reel numbers its segments monotonically, so the
/// number alone names the file.
type CacheKey = SegmentId;

struct CacheEntry {
    handle: SegmentHandle,
    is_hot: AtomicBool,
}

/// A bounded cache of open sealed segment handles, reclaimed by second chance
///
/// Eviction drops the cache's reference only; a reader still holding the handle
/// keeps the file open. A hit takes a shared guard and sets one recency bit, so
/// only an insert that has to make room takes the map exclusively.
pub struct FdCache {
    capacity: usize,
    id: u64,
    entries: RwLock<HashMap<CacheKey, CacheEntry, SegmentIdHash>>,
}

/// Names the next cache, so two volumes never share a memo
static NEXT_CACHE_ID: AtomicU64 = AtomicU64::new(0);

/// The last handle a thread resolved, answered without the map's guard
///
/// The reference is weak, so the memo never keeps a file alive: a doomed segment
/// unlinks the moment its real readers drain, and an upgrade that fails falls
/// through to the map.
struct HandleMemo {
    cache: u64,
    segment: SegmentId,
    handle: std::sync::Weak<SegmentInner>,
}

thread_local! {
    static LAST_HANDLE: RefCell<Option<HandleMemo>> = const { RefCell::new(None) };
}

/// Hash for a key this reel issues and no caller chooses
///
/// A segment id is a counter this volume allocates in order, so SipHash has no
/// adversary to defend against. The multiply spreads the low bits the counter
/// concentrates, since the table takes its bucket from those.
#[derive(Clone, Copy, Default)]
pub struct SegmentIdHash;

impl std::hash::BuildHasher for SegmentIdHash {
    type Hasher = SegmentIdHasher;

    fn build_hasher(&self) -> SegmentIdHasher {
        SegmentIdHasher(0)
    }
}

/// The hasher above, holding the mix as it goes
#[derive(Default)]
pub struct SegmentIdHasher(u64);

/// The multiplier, 2^64 divided by the golden ratio
const ID_MIX: u64 = 0x9e37_79b9_7f4a_7c15;

impl std::hash::Hasher for SegmentIdHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    /// The byte-at-a-time fallback, which a `SegmentId` never reaches
    fn write(&mut self, bytes: &[u8]) {
        for &byte in bytes {
            self.0 = (self.0 ^ u64::from(byte)).wrapping_mul(ID_MIX);
        }
        self.0 ^= self.0 >> 32;
    }

    fn write_u32(&mut self, value: u32) {
        let mixed = u64::from(value).wrapping_mul(ID_MIX);
        self.0 = mixed ^ (mixed >> 32);
    }
}

impl FdCache {
    /// A cache holding at most this many sealed handles, floored at one
    pub fn new(capacity: usize) -> FdCache {
        FdCache {
            capacity: capacity.max(1),
            id: NEXT_CACHE_ID.fetch_add(1, Ordering::Relaxed),
            entries: RwLock::new(HashMap::default()),
        }
    }

    /// Fetch a cached handle, giving it another chance at the next sweep
    ///
    /// The thread's memo answers first, and a segment id is never reused within a
    /// volume, so a memo that matches the cache cannot answer with the wrong bytes.
    pub fn get(&self, id: SegmentId) -> Option<SegmentHandle> {
        let memoized = LAST_HANDLE.with(|slot| {
            slot.borrow()
                .as_ref()
                .filter(|memo| memo.cache == self.id && memo.segment == id)
                .and_then(|memo| memo.handle.upgrade())
                .map(|inner| SegmentHandle { inner })
        });
        if memoized.is_some() {
            return memoized;
        }

        let handle = {
            let entries = read(&self.entries);
            let entry = entries.get(&id)?;
            if !entry.is_hot.load(Ordering::Relaxed) {
                entry.is_hot.store(true, Ordering::Relaxed);
            }
            entry.handle.clone()
        };
        LAST_HANDLE.with(|slot| {
            *slot.borrow_mut() = Some(HandleMemo {
                cache: self.id,
                segment: id,
                handle: Arc::downgrade(&handle.inner),
            });
        });
        Some(handle)
    }

    /// Insert a handle, sweeping for a cold entry when at capacity
    pub fn insert(&self, handle: SegmentHandle) {
        let key = handle.id();
        let mut entries = write(&self.entries);
        if !entries.contains_key(&key) && entries.len() >= self.capacity {
            evict_cold(&mut entries);
        }
        entries.insert(
            key,
            CacheEntry {
                handle,
                is_hot: AtomicBool::new(false),
            },
        );
    }

    /// Drop a doomed segment from the cache when it is doomed
    pub fn remove(&self, id: SegmentId) -> Option<SegmentHandle> {
        write(&self.entries).remove(&id).map(|entry| entry.handle)
    }

    /// Drop every cached handle, for a reader rebuilding its view of the volume
    pub fn clear(&self) {
        write(&self.entries).clear();
    }

    /// Number of handles currently cached
    pub fn len(&self) -> usize {
        read(&self.entries).len()
    }

    /// Whether the cache holds no handles
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Drop one entry that has not been read since the last sweep passed it
///
/// A pass where every entry is hot clears them all and takes the first one it
/// reached, so making room always makes room.
fn evict_cold(entries: &mut HashMap<CacheKey, CacheEntry, SegmentIdHash>) {
    let mut fallback = None;
    let mut cold = None;
    for (key, entry) in entries.iter() {
        if fallback.is_none() {
            fallback = Some(*key);
        }
        if !entry.is_hot.swap(false, Ordering::Relaxed) {
            cold = Some(*key);
            break;
        }
    }
    if let Some(key) = cold.or(fallback) {
        entries.remove(&key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::future::Future;
    use std::sync::atomic::AtomicUsize;
    use std::task::{Context, Poll, Wake, Waker};
    use std::thread;
    use std::time::Duration;

    use tempfile::tempdir;

    use crate::io::fault::{FaultKind, FaultPlan};
    use crate::io::posix_backend::PosixBackend;
    use crate::io::sim_backend::SimIo;
    use crate::io::slots::SLOT_COUNT;
    use crate::sync::tension::block_on;

    /// A waker that counts what it takes, for a poll that must leave none
    struct Counter(AtomicUsize);

    impl Wake for Counter {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn driver() -> Arc<IoDriver> {
        driver_under(FaultPlan::new(1))
    }

    fn driver_under(plan: FaultPlan) -> Arc<IoDriver> {
        Arc::new(IoDriver::new(Arc::new(SimIo::new(plan))))
    }

    fn open(driver: &IoDriver, path: &Path) -> FileId {
        driver.open(path, true).expect("open")
    }

    fn list(driver: &IoDriver, dir: &Path) -> Vec<SegmentEntry> {
        driver.list(dir).expect("list")
    }

    /// A file holding these bytes, for the reads the futures fly
    fn written(driver: &IoDriver, path: &Path, bytes: &[u8]) -> FileId {
        let file = open(driver, path);
        driver
            .writev(file, 0, vec![WriteBuf::owned(bytes.to_vec())])
            .expect("write");
        file
    }

    fn read_op(driver: &IoDriver, file: FileId, len: usize) -> Op {
        Op::Pread {
            tag: driver.next_tag(),
            file,
            offset: 0,
            buf: ReadBuf::new(len),
        }
    }

    /// The answer a poll gave, absent while it is still pending
    fn ready<Answer>(polled: Poll<Answer>) -> Option<Answer> {
        match polled {
            Poll::Ready(answer) => Some(answer),
            Poll::Pending => None,
        }
    }

    /// What a read completion filled, for a test that asked for a read
    fn filled(completion: Completion) -> Option<Vec<u8>> {
        match completion.outcome {
            Outcome::Read { result, buf } => result.ok().map(|_| buf.into_vec()),
            Outcome::Opened(_)
            | Outcome::Wrote { .. }
            | Outcome::ReadSplit { .. }
            | Outcome::Listed(_)
            | Outcome::Length(_)
            | Outcome::Done(_) => None,
        }
    }

    // the driver returns completions in the order the ops were submitted
    #[test]
    fn run_preserves_order() {
        let driver = driver();
        let dir = Path::new("/reel");

        let completions = driver
            .run(vec![
                Op::Open {
                    tag: Tag(10),
                    path: dir.join("a"),
                    create: true,
                    direct: false,
                },
                Op::Open {
                    tag: Tag(11),
                    path: dir.join("b"),
                    create: true,
                    direct: false,
                },
            ])
            .expect("run");

        assert_eq!(completions[0].tag, Tag(10));
        assert_eq!(completions[1].tag, Tag(11));
    }

    // a doomed segment is unlinked only when the last handle drops
    #[test]
    fn unlink_on_last_drop() {
        let driver = driver();
        let dir = Path::new("/reel");
        let path = dir.join("000001.reel");
        let file = open(&driver, &path);

        let handle = SegmentHandle::new(SegmentId(1), path.clone(), file, Arc::clone(&driver));
        let reader = handle.clone();
        handle.mark_doomed();

        drop(handle);
        assert_eq!(list(&driver, dir).len(), 1);

        drop(reader);
        assert!(list(&driver, dir).is_empty());
    }

    // the memo answers a repeat without the map, pins nothing, and never answers
    // for another cache
    #[test]
    fn a_repeat_resolution_is_memoized_without_pinning() {
        let driver = driver();
        let dir = Path::new("/reel");
        let path = dir.join("000009.reel");
        let file = open(&driver, &path);
        let handle = SegmentHandle::new(SegmentId(9), path, file, Arc::clone(&driver));

        let cache = FdCache::new(4);
        cache.insert(handle.clone());

        let first = cache.get(SegmentId(9)).expect("first get");
        let repeat = cache.get(SegmentId(9)).expect("memoized get");
        assert_eq!(first.id(), repeat.id());

        // Removal drops the map's reference, leaving the weak memo nothing to
        // upgrade once the real readers drain.
        cache.remove(SegmentId(9));
        drop(handle);
        drop(first);
        drop(repeat);
        assert!(
            cache.get(SegmentId(9)).is_none(),
            "nothing left to upgrade once readers drain"
        );

        // A second cache on the same thread cannot be answered by the first one's
        // memo, whatever ids it holds.
        let other = FdCache::new(4);
        assert!(other.get(SegmentId(9)).is_none());
    }

    // a live segment that is never doomed survives every handle drop
    #[test]
    fn live_segment_survives() {
        let driver = driver();
        let dir = Path::new("/reel");
        let path = dir.join("000002.reel");
        let file = open(&driver, &path);

        let handle = SegmentHandle::new(SegmentId(2), path, file, Arc::clone(&driver));
        assert_eq!(handle.reference_count(), 1);

        drop(handle);
        assert_eq!(list(&driver, dir).len(), 1);
    }

    // the cache evicts the least recently used handle at capacity
    #[test]
    fn evicts_least_recently_used() {
        let driver = driver();
        let dir = Path::new("/reel");
        let cache = FdCache::new(2);

        for number in 1..=2u32 {
            let path = dir.join(format!("{number}.reel"));
            let file = open(&driver, &path);
            cache.insert(SegmentHandle::new(
                SegmentId(number),
                path,
                file,
                Arc::clone(&driver),
            ));
        }

        assert!(cache.get(SegmentId(1)).is_some());

        let path = dir.join("3.reel");
        let file = open(&driver, &path);
        cache.insert(SegmentHandle::new(
            SegmentId(3),
            path,
            file,
            Arc::clone(&driver),
        ));

        assert_eq!(cache.len(), 2);
        assert!(cache.get(SegmentId(2)).is_none());
        assert!(cache.get(SegmentId(1)).is_some());
        assert!(cache.get(SegmentId(3)).is_some());
    }

    // removing a doomed segment takes it out of the cache
    #[test]
    fn remove_clears_entry() {
        let driver = driver();
        let path = Path::new("/reel").join("9.reel");
        let file = open(&driver, &path);
        let cache = FdCache::new(4);

        cache.insert(SegmentHandle::new(
            SegmentId(9),
            path,
            file,
            Arc::clone(&driver),
        ));
        assert!(cache.remove(SegmentId(9)).is_some());

        assert!(cache.is_empty());
        assert!(cache.get(SegmentId(9)).is_none());
    }

    // a batch wider than the table goes down in runs rather than all at once
    #[test]
    fn a_wide_batch_goes_down_in_runs() {
        let sim = SimIo::new(FaultPlan::new(1));
        let driver = Arc::new(IoDriver::new(Arc::new(sim.clone())));
        let file = written(&driver, &Path::new("/reel").join("wide.reel"), b"payload");
        let wanted = driver.slots() + 8;

        let ops: Vec<Op> = (0..wanted).map(|_| read_op(&driver, file, 4)).collect();
        let completions = driver.run(ops).expect("run");

        assert_eq!(completions.len(), wanted);
        assert!(
            sim.widest_batch() <= driver.slots(),
            "a submission went past the table's width"
        );
        assert_eq!(driver.outstanding(), 0);
    }

    // a future left pending resolves when another thread reaps its completion
    #[test]
    fn a_pending_future_resolves_out_of_band() {
        let driver = driver();
        let file = written(&driver, &Path::new("/reel").join("wait.reel"), b"payload");

        // The door serves itself, so the out-of-band path has to be forced: with
        // the self-drain seam refused, a thread that submitted nothing drains.
        let script = crate::sync::rendezvous::script();
        script.refuse("slots/self-drain");

        let reaper = Arc::clone(&driver);
        let reap = thread::spawn(move || {
            while reaper.reap().expect("reap") == 0 {
                thread::sleep(Duration::from_millis(1));
            }
        });

        let completion = block_on(driver.wait_op(read_op(&driver, file, 7))).expect("submit");
        reap.join().expect("the reaper joins");

        assert_eq!(filled(completion).expect("a read came back"), b"payload");
        assert_eq!(driver.outstanding(), 0);
        assert_eq!(driver.wakers(), 0);
    }

    // completions drained out of order land in their own slots
    #[test]
    fn reordered_completions_land_home() {
        let driver = driver_under(FaultPlan::new(1).with_reorder());
        let file = written(
            &driver,
            &Path::new("/reel").join("reorder.reel"),
            b"payload",
        );
        let first_op = read_op(&driver, file, 3);
        let second_op = read_op(&driver, file, 7);
        let (first_tag, second_tag) = (first_op.tag(), second_op.tag());

        // Refused, so both futures stay pending until the reap: reordering only
        // means anything once both completions queue behind one drain.
        let script = crate::sync::rendezvous::script();
        script.refuse("slots/self-drain");

        let mut first = Box::pin(driver.wait_op(first_op));
        let mut second = Box::pin(driver.wait_op(second_op));
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);
        assert!(first.as_mut().poll(&mut cx).is_pending());
        assert!(second.as_mut().poll(&mut cx).is_pending());

        assert_eq!(driver.reap().expect("reap"), 2);

        let landed_first = ready(first.as_mut().poll(&mut cx))
            .expect("the first read landed")
            .expect("submit");
        let landed_second = ready(second.as_mut().poll(&mut cx))
            .expect("the second read landed")
            .expect("submit");
        assert_eq!(landed_first.tag, first_tag);
        assert_eq!(landed_second.tag, second_tag);
        assert_eq!(driver.outstanding(), 0);
    }

    // a future polled again while pending keeps exactly one waker
    #[test]
    fn a_repolled_future_holds_one_seat() {
        let driver = driver();
        let file = written(&driver, &Path::new("/reel").join("repoll.reel"), b"payload");

        // Refused, so the polls stay pending and the seat count is askable.
        let script = crate::sync::rendezvous::script();
        script.refuse("slots/self-drain");

        let mut waiting = Box::pin(driver.wait_op(read_op(&driver, file, 7)));
        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);
        assert!(waiting.as_mut().poll(&mut cx).is_pending());
        assert!(waiting.as_mut().poll(&mut cx).is_pending());

        assert_eq!(driver.wakers(), 1);

        driver.reap().expect("reap");
        assert!(waiting.as_mut().poll(&mut cx).is_ready());
        assert_eq!(driver.wakers(), 0);
        assert_eq!(driver.outstanding(), 0);
    }

    // futures dropped mid flight give back their slots and their buffers
    #[test]
    fn dropped_futures_leak_nothing() {
        // Refused, so the futures stay pending on the passive path.
        let script = crate::sync::rendezvous::script();
        script.refuse("slots/self-drain");
        let driver = driver();
        let file = written(
            &driver,
            &Path::new("/reel").join("dropped.reel"),
            b"payload",
        );
        let held_before = crate::reel::payload::held_bytes();

        {
            let waker = Waker::noop();
            let mut cx = Context::from_waker(waker);
            let mut abandoned = Vec::new();
            for _ in 0..4 {
                let mut waiting = Box::pin(driver.wait_op(read_op(&driver, file, 4096)));
                assert!(waiting.as_mut().poll(&mut cx).is_pending());
                abandoned.push(waiting);
            }
            assert_eq!(driver.outstanding(), 4);
            assert_eq!(driver.wakers(), 4);
        }

        assert_eq!(driver.outstanding(), 4, "a dropped flight keeps its seat");
        assert_eq!(
            driver.wakers(),
            0,
            "a dropped future takes its waker with it"
        );

        driver.reap().expect("reap");

        assert_eq!(driver.outstanding(), 0, "the table came back empty");
        assert_eq!(driver.reclaimed(), 4);
        assert!(
            crate::reel::payload::held_bytes() >= held_before,
            "an abandoned read buffer went to the allocator rather than the pool"
        );
    }

    // a batch future answers in submit order under reordered and delayed completions
    #[test]
    fn a_batch_future_survives_faults() {
        // Refused, so the futures stay pending on the passive path.
        let script = crate::sync::rendezvous::script();
        script.refuse("slots/self-drain");
        let plan = FaultPlan::new(1)
            .with_reorder()
            .with_fault(3, FaultKind::DelayCompletion { polls: 2 });
        let driver = driver_under(plan);
        let file = written(&driver, &Path::new("/reel").join("batch.reel"), b"payload");

        let ops = vec![
            read_op(&driver, file, 3),
            read_op(&driver, file, 5),
            read_op(&driver, file, 7),
        ];
        let wanted: Vec<Tag> = ops.iter().map(|op| op.tag()).collect();

        let mut waiting = Box::pin(driver.wait_batch(ops));
        let woken = Arc::new(Counter(AtomicUsize::new(0)));
        let waker = Waker::from(Arc::clone(&woken));
        let mut cx = Context::from_waker(&waker);
        assert!(waiting.as_mut().poll(&mut cx).is_pending());

        driver.reap().expect("first reap");
        assert!(
            waiting.as_mut().poll(&mut cx).is_pending(),
            "the delayed read has not landed yet"
        );
        driver.reap().expect("second reap");

        let landed = ready(waiting.as_mut().poll(&mut cx))
            .expect("the batch landed")
            .expect("submit");
        let answered: Vec<Tag> = landed.iter().map(|completion| completion.tag).collect();
        assert_eq!(answered, wanted, "the batch answered out of submit order");
        assert!(
            woken.0.load(Ordering::SeqCst) >= 1,
            "no reap woke the batch"
        );
        assert_eq!(driver.outstanding(), 0);
        assert_eq!(driver.wakers(), 0);
    }

    // a batch future dropped mid flight orphans its whole range
    #[test]
    fn a_dropped_batch_orphans_its_range() {
        // Refused, so the futures stay pending on the passive path.
        let script = crate::sync::rendezvous::script();
        script.refuse("slots/self-drain");
        let plan = FaultPlan::new(1).with_fault(3, FaultKind::DelayCompletion { polls: 2 });
        let driver = driver_under(plan);
        let file = written(
            &driver,
            &Path::new("/reel").join("drop-batch.reel"),
            b"payload",
        );

        {
            let ops = vec![
                read_op(&driver, file, 3),
                read_op(&driver, file, 5),
                read_op(&driver, file, 7),
            ];
            let mut waiting = Box::pin(driver.wait_batch(ops));
            let waker = Waker::noop();
            let mut cx = Context::from_waker(waker);
            assert!(waiting.as_mut().poll(&mut cx).is_pending());

            // Two of the three are in hand and the delayed one is still out, so
            // the drop has both shapes to put back.
            driver.reap().expect("reap");
            assert!(waiting.as_mut().poll(&mut cx).is_pending());
        }

        driver.reap().expect("reap the delayed read");

        assert_eq!(driver.outstanding(), 0);
        assert_eq!(driver.reclaimed(), 3);
        assert_eq!(driver.wakers(), 0);
    }

    // a backend that answers at submission leaves the future nothing to wait on
    #[test]
    fn an_inline_backend_answers_at_the_first_poll() {
        let dir = tempdir().expect("tempdir");
        let backend = PosixBackend::new();
        let driver = Arc::new(IoDriver::new(Arc::new(backend)));
        let path = dir.path().join("inline.reel");

        let op = Op::Open {
            tag: driver.next_tag(),
            path,
            create: true,
            direct: false,
        };
        let mut waiting = Box::pin(driver.wait_op(op));
        let woken = Arc::new(Counter(AtomicUsize::new(0)));
        let waker = Waker::from(Arc::clone(&woken));
        let mut cx = Context::from_waker(&waker);

        let completion = ready(waiting.as_mut().poll(&mut cx))
            .expect("an inline backend answered at submission")
            .expect("submit");
        assert!(matches!(completion.outcome, Outcome::Opened(Ok(_))));
        assert_eq!(driver.wakers(), 0, "the first poll left a waker behind");
        assert_eq!(woken.0.load(Ordering::SeqCst), 0);
        assert_eq!(driver.outstanding(), 0);
    }

    // a wrapped claim yields to the caller rather than parking it
    #[test]
    fn a_wrapped_claim_yields_rather_than_parking() {
        // Refused, so the futures stay pending on the passive path.
        let script = crate::sync::rendezvous::script();
        script.refuse("slots/self-drain");
        let driver = driver();
        let file = written(
            &driver,
            &Path::new("/reel").join("wrapped.reel"),
            b"payload",
        );
        let woken = Arc::new(Counter(AtomicUsize::new(0)));
        let waker = Waker::from(Arc::clone(&woken));
        let mut cx = Context::from_waker(&waker);

        // The holder's completion lands in its slot and stays there, and the tags
        // burnt after it leave the next op exactly one table on.
        let holder = read_op(&driver, file, 7);
        let at = holder.tag();
        let mut holding = Box::pin(driver.wait_op(holder));
        assert!(holding.as_mut().poll(&mut cx).is_pending());
        driver.reap().expect("reap the holder");
        while driver.next_tag().0 + 1 < at.0 + SLOT_COUNT as u64 {}

        let op = read_op(&driver, file, 7);
        assert_eq!(op.tag().0, at.0 + SLOT_COUNT as u64);
        let mut wrapped = Box::pin(driver.wait_op(op));
        assert!(
            wrapped.as_mut().poll(&mut cx).is_pending(),
            "the wrapped claim took a slot the holder is carrying"
        );

        // The take the claim is waiting for, which nothing but this thread makes.
        let landed = ready(holding.as_mut().poll(&mut cx))
            .expect("the holder landed")
            .expect("submit");
        assert_eq!(filled(landed).expect("a read came back"), b"payload");
        assert!(
            woken.0.load(Ordering::SeqCst) >= 1,
            "the freed slot rang nothing waiting for it"
        );

        assert!(
            wrapped.as_mut().poll(&mut cx).is_pending(),
            "the wrapped read went down and is waiting on the backend"
        );
        driver.reap().expect("reap the wrapped read");
        let answered = ready(wrapped.as_mut().poll(&mut cx))
            .expect("the wrapped read landed")
            .expect("submit");

        assert_eq!(filled(answered).expect("a read came back"), b"payload");
        assert_eq!(driver.outstanding(), 0);
    }

    // caller threads filing their own completions still each take their own
    #[test]
    fn concurrent_inline_callers_keep_their_own_completions() {
        let dir = tempdir().expect("tempdir");
        let driver = Arc::new(IoDriver::new(Arc::new(PosixBackend::new())));
        let readers = 4;
        let batch = driver.batch_slots();
        let rounds = 8;

        let mut running = Vec::with_capacity(readers);
        for reader in 0..readers {
            let driver = Arc::clone(&driver);
            let path = dir.path().join(format!("caller-{reader}.reel"));
            running.push(thread::spawn(move || {
                let mark = [b'a' + reader as u8; 8];
                let file = written(&driver, &path, &mark);
                for _ in 0..rounds {
                    let ops: Vec<Op> = (0..batch)
                        .map(|_| read_op(&driver, file, mark.len()))
                        .collect();
                    let answered = block_on(driver.wait_batch(ops)).expect("submit the batch");
                    for completion in answered {
                        assert_eq!(filled(completion).expect("a read came back"), mark);
                    }
                }
            }));
        }

        for reader in running {
            reader.join().expect("a reader joins");
        }
        assert_eq!(driver.outstanding(), 0);
        assert_eq!(driver.reclaimed(), 0);
    }

    // an inline batch answers at submission too, whole and in order
    #[test]
    fn an_inline_batch_answers_at_the_first_poll() {
        let dir = tempdir().expect("tempdir");
        let driver = Arc::new(IoDriver::new(Arc::new(PosixBackend::new())));
        let path = dir.path().join("inline-batch.reel");
        let file = written(&driver, &path, b"payload");

        let ops = vec![
            read_op(&driver, file, 3),
            read_op(&driver, file, 5),
            read_op(&driver, file, 7),
        ];
        let wanted: Vec<Tag> = ops.iter().map(|op| op.tag()).collect();
        let mut waiting = Box::pin(driver.wait_batch(ops));
        let woken = Arc::new(Counter(AtomicUsize::new(0)));
        let waker = Waker::from(Arc::clone(&woken));
        let mut cx = Context::from_waker(&waker);

        let landed = ready(waiting.as_mut().poll(&mut cx))
            .expect("the batch answered inline")
            .expect("submit");
        let answered: Vec<Tag> = landed.iter().map(|completion| completion.tag).collect();
        assert_eq!(answered, wanted);
        assert_eq!(driver.wakers(), 0, "the first poll left a waker behind");
        assert_eq!(woken.0.load(Ordering::SeqCst), 0);
        assert_eq!(driver.outstanding(), 0);
    }
}
