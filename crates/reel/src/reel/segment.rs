//! Refcounted segment handles that unlink on last drop, the bounded fd cache, and the io driver

use std::cell::RefCell;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use crate::error::{ReelError, Result};
use crate::format::loc::SegmentId;
use crate::format::record::{RecordHeader, RecordLayout, HEADER_LEN};
use crate::format::segment_header::{SegmentHeader, SEGMENT_HEADER_SPAN};
use crate::hold::{segment_key, Hold};
use crate::io::mapping::Mapping;
use crate::io::op::{
    Advice, Completion, FileId, Op, Outcome, Part, ReadBuf, SegmentEntry, Tag, WarmFirst, WriteBuf,
};
use crate::io::slots::{runs_of, SlotTable};
use crate::io::ReelIo;

/// What one read of a batch filled, or why that read alone could not be served
pub type SplitRead = std::result::Result<(Vec<u8>, Vec<u8>), ReelError>;

/// What one split read filled, or its error and the spare buffer it came in with
pub type SplitAnswer = std::result::Result<(Vec<u8>, Vec<u8>), (ReelError, Vec<u8>)>;

/// How many empty poll rounds the driver waits for a completion before giving up
const MAX_POLL_ROUNDS: u32 = 1_000_000;

/// The driver spins this many empty poll rounds before it yields the core
const SPIN_ROUNDS: u32 = 64;

/// Correlates concurrent submitters with one backend's shared completion queue
pub struct IoDriver {
    /// The backend every op goes down to
    backend: Arc<dyn ReelIo>,

    /// The slots every op lands in, shared so a backend's own threads can file into them
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

    /// Which backend is serving under this driver
    pub fn serving(&self) -> crate::io::ServingBackend {
        self.backend.serving()
    }

    /// How many device flushes the backend under this driver has asked for
    pub fn sync_count(&self) -> u64 {
        self.backend.sync_count()
    }

    /// Which door the backend's ops took, ring or the fallback beside it
    pub fn door_counts(&self) -> crate::io::DoorCounts {
        self.backend.door_counts()
    }

    /// Nanoseconds the backend spent waiting inside its flushes
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

    /// How many ops one awaited batch asks for at a time, a quarter of the table
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

    /// Completions put back undelivered, which is what a dropped future costs
    pub fn reclaimed(&self) -> u64 {
        self.slots.reclaimed()
    }

    /// Drain ready completions into their slots, zero when another thread is already draining
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
    pub fn run_into(&self, ops: &mut Vec<Op>, out: &mut Vec<Completion>) -> Result<()> {
        out.clear();
        // A backend that runs the batch on this thread answers in submit order, with no slots
        if self.backend.submit_batch(ops, out) {
            return Ok(());
        }

        let wanted: Vec<Tag> = ops.iter().map(|op| op.tag()).collect();
        let mut filled: Vec<Option<Completion>> = Vec::new();
        filled.resize_with(wanted.len(), || None);

        // A batch wider than the table goes down in runs as earlier runs free their slots
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

    /// Submit one op and collect its completion, answered in place by an inline backend
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
    pub async fn wait_op(&self, op: Op) -> Result<Completion> {
        let wanted = [op.tag()];
        self.slots.claim(&wanted).await;
        if let Err(error) = self.backend.submit_detached_one(op, &self.slots) {
            self.slots.release_run(&wanted);
            return Err(error);
        }
        // A pending poll drains the backend itself, with its waker seated before the drain
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

    /// Submit a batch and await its completions in submit order, claiming all slots at once
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
        // A pending batch drains the backend itself, the same as a single op
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

    /// Claim slots for as much of a batch as the table has room for, waiting for at least one
    fn claim_run(&self, wanted: &[Tag]) -> Result<usize> {
        self.slots.wait_free(
            || {
                let run = self.slots.claim_run(wanted);
                (run > 0).then_some(run)
            },
            |scratch| self.backend.poll(scratch),
        )
    }

    /// Wait until every wanted tag has landed, orphaning the rest if the wait gives up
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

    /// Open or create a file, returning the handle later ops use
    pub fn open(&self, path: &Path, create: bool) -> Result<FileId> {
        let op = Op::Open {
            tag: self.next_tag(),
            path: path.to_path_buf(),
            create,
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
        warm: WarmFirst,
    ) -> Result<Vec<u8>> {
        if len == 0 {
            return Ok(Vec::new());
        }
        let mut reuse = reuse;
        if warm == WarmFirst::Ask {
            // The probe is a split read, so the window is its payload behind an empty head
            let mut head = ReadBuf::new(0);
            let mut body = ReadBuf::reusing(reuse, len as usize);
            if self.backend.warm_split(file, offset, &mut head, &mut body) {
                return Ok(body.into_vec());
            }
            reuse = body.into_vec();
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

    /// Read one range into a header buffer and a payload buffer, parking the caller
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

    /// Read one range from resident pages only, or nothing when any of it is cold
    pub fn warm_only(&self, file: FileId, offset: u64, len: usize) -> Option<Vec<u8>> {
        let mut bytes = ReadBuf::new(len);
        let mut nothing = ReadBuf::new(0);
        match self
            .backend
            .warm_split(file, offset, &mut bytes, &mut nothing)
        {
            true => Some(bytes.into_vec()),
            false => None,
        }
    }

    /// Read one split range as a future, asking the page cache first on `WarmFirst::Ask`
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

    /// Run a batch of split reads, one failed read in its slot and a failed batch as the error
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
    pub async fn wait_split_reads(
        &self,
        mut ops: Vec<Op>,
        warm: WarmFirst,
    ) -> Result<Vec<SplitRead>> {
        let mut filled = Vec::with_capacity(ops.len());
        self.wait_split_reads_into(&mut ops, &mut filled, warm)
            .await?;
        Ok(filled)
    }

    /// The same awaited batch, taking the ops out of a vector the caller keeps
    pub async fn wait_split_reads_into(
        &self,
        ops: &mut Vec<Op>,
        filled: &mut Vec<SplitRead>,
        warm: WarmFirst,
    ) -> Result<()> {
        filled.clear();
        if warm == WarmFirst::Skip {
            while !ops.is_empty() {
                let run = ops.len().min(self.batch_slots());
                let batch: Vec<Op> = ops.drain(..run).collect();
                collect_split_reads(self.wait_batch(batch).await?, filled)?;
            }
            return Ok(());
        }
        let mut cold: Vec<(usize, Op)> = Vec::new();
        for (at, op) in ops.drain(..).enumerate() {
            match self.warm_op(op) {
                Ok(read) => filled.push(read),
                Err(op) => {
                    cold.push((at, op));
                    filled.push(Ok((Vec::new(), Vec::new())));
                }
            }
        }
        while !cold.is_empty() {
            let run = cold.len().min(self.batch_slots());
            let (places, batch): (Vec<usize>, Vec<Op>) = cold.drain(..run).unzip();
            let mut landed = Vec::with_capacity(run);
            collect_split_reads(self.wait_batch(batch).await?, &mut landed)?;
            for (place, read) in places.into_iter().zip(landed) {
                filled[place] = read;
            }
        }
        Ok(())
    }

    /// One split read answered from the page cache, or the op back to go down with the batch
    fn warm_op(&self, op: Op) -> std::result::Result<SplitRead, Op> {
        let Op::PreadSplit {
            tag,
            file,
            offset,
            mut head,
            mut body,
        } = op
        else {
            return Err(op);
        };
        if self.backend.warm_split(file, offset, &mut head, &mut body) {
            return Ok(Ok((head.into_vec(), body.into_vec())));
        }
        Err(Op::PreadSplit {
            tag,
            file,
            offset,
            head,
            body,
        })
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

    /// The same write, failing if it landed short of what it framed
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

    /// List a directory, reporting a missing one as empty and any other failure as an error
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

    /// Tell the kernel how a range will be read, a zero length covering the whole file
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

    /// Reserve space ahead of the write head without extending the length
    pub fn allocate(&self, file: FileId, offset: u64, len: u64) -> Result<()> {
        self.done(Op::Allocate {
            tag: self.next_tag(),
            file,
            offset,
            len,
        })
    }

    /// Give back the blocks under a byte range of zeros, keeping the length
    pub fn release(&self, file: FileId, offset: u64, len: u64) -> Result<()> {
        if len == 0 {
            return Ok(());
        }
        self.done(Op::Release {
            tag: self.next_tag(),
            file,
            offset,
            len,
        })
    }

    /// Cut a file to a length, handing reserved space past it back
    pub fn truncate(&self, file: FileId, len: u64) -> Result<()> {
        self.done(Op::Truncate {
            tag: self.next_tag(),
            file,
            len,
        })
    }

    /// Run one op whose completion has no payload
    fn done(&self, op: Op) -> Result<()> {
        match self.run_op(op)?.outcome {
            Outcome::Done(result) => result,
            other => Err(wrong_shape(&other)),
        }
    }
}

/// What a split read's completion filled, or its error and the spare header buffer
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

/// A segment reader fetches this many bytes per trip to the backend
pub(crate) const READ_CHUNK: usize = 1 << 20;

/// The handle a tail holds before it opens anything, a number no backend issues
const NO_FILE: FileId = FileId(u64::MAX);

/// A forward-only reader over one segment that reads its bytes a window at a time
pub struct SegmentReader<'driver> {
    driver: &'driver IoDriver,
    file: FileId,
    limit: u64,
    window: Arc<Vec<u8>>,
    window_at: u64,
    read_bytes: u64,

    spare: Vec<Vec<u8>>,

    lent: Vec<Arc<Vec<u8>>>,
}

impl<'driver> SegmentReader<'driver> {
    /// A reader over the first bytes of one open segment
    pub fn new(driver: &'driver IoDriver, file: FileId, limit: u64) -> SegmentReader<'driver> {
        SegmentReader {
            driver,
            file,
            limit,
            window: Arc::default(),
            window_at: 0,
            read_bytes: 0,
            spare: Vec::new(),
            lent: Vec::new(),
        }
    }

    /// Hand the reader buffers an earlier reader finished with
    pub fn stock(&mut self, spare: Vec<Vec<u8>>) {
        self.spare = spare;
    }

    /// Every buffer the reader holds alone, for the next reader to start with
    pub fn into_spare(mut self) -> Vec<Vec<u8>> {
        self.retire(Arc::default());
        self.reclaim();
        self.spare
    }

    /// The payload at this offset as a stretch of the window, kept alive while a write holds it
    pub fn held(&mut self, offset: u64, len: usize) -> Result<Part> {
        let len = self.range(offset, len)?.len();
        let start = match len {
            0 => 0,
            _ => (offset - self.window_at) as usize,
        };
        Ok(Part::new(&self.window, start, len))
    }

    /// Swap the window out, keeping its buffer where nothing else holds it
    fn retire(&mut self, window: Arc<Vec<u8>>) {
        match Arc::try_unwrap(std::mem::replace(&mut self.window, window)) {
            Ok(bytes) if bytes.capacity() > 0 => self.spare.push(bytes),
            Ok(_) => {}
            Err(held) => self.lent.push(held),
        }
    }

    /// Take back every lent window the writes have let go of
    fn reclaim(&mut self) {
        let mut at = 0;
        while at < self.lent.len() {
            if Arc::strong_count(&self.lent[at]) > 1 {
                at += 1;
                continue;
            }
            if let Ok(bytes) = Arc::try_unwrap(self.lent.swap_remove(at)) {
                self.spare.push(bytes);
            }
        }
    }

    /// A buffer for the next read, one already paid for where there is one
    fn buffer(&mut self) -> Vec<u8> {
        if self.spare.is_empty() {
            self.reclaim();
        }
        self.spare.pop().unwrap_or_default()
    }

    /// The offset the reader stops at
    pub fn limit(&self) -> u64 {
        self.limit
    }

    /// Bytes the reader has fetched from the backend across every refill
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
        self.retire(Arc::default());
        let reuse = self.buffer();
        self.window = Arc::new(self.driver.pread_reusing(self.file, offset, span, reuse)?);
        self.window_at = offset;
        self.read_bytes += self.window.len() as u64;
        Ok(())
    }

    /// Bytes several ranges hold, fetched as one batch and returned in ask order
    pub fn read_ranges(&mut self, ranges: &[(u64, usize)]) -> Result<Vec<Vec<u8>>> {
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
                buf: ReadBuf::reusing(self.buffer(), wanted),
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
    pub fn preload(&mut self, offset: u64, window: Vec<u8>) {
        self.window_at = offset;
        self.retire(Arc::new(window));
    }
}

struct SegmentInner {
    id: SegmentId,
    path: PathBuf,
    file: FileId,
    driver: Arc<IoDriver>,
    layout: RecordLayout,
    is_doomed: AtomicBool,
    mapping: OnceLock<Option<Arc<Mapping>>>,
}

impl Drop for SegmentInner {
    /// Unlink a doomed segment, then release the descriptor either way
    fn drop(&mut self) {
        if self.file == NO_FILE {
            return;
        }
        if self.is_doomed.load(Ordering::Acquire) {
            if let Err(error) = self.driver.unlink(&self.path) {
                tracing::warn!("failed to unlink a doomed reel segment on last drop: {error}");
            }
        }
        if let Err(error) = self.driver.close(self.file) {
            tracing::warn!("failed to release a reel segment descriptor on last drop: {error}");
        }
    }
}

/// The header record a segment file opens with, or nothing when it has none
pub fn read_segment_header(driver: &IoDriver, file: FileId) -> Result<Option<SegmentHeader>> {
    let head = driver.pread(file, 0, (HEADER_LEN + SEGMENT_HEADER_SPAN) as u64)?;
    let Ok(header) = RecordHeader::unpack(head.get(..HEADER_LEN).unwrap_or(&[])) else {
        return Ok(None);
    };
    let end = HEADER_LEN + header.length as usize;
    let Some(payload) = head.get(HEADER_LEN..end) else {
        return Ok(None);
    };
    if !header.flags.is_segment_header() || !header.verify(payload) {
        return Ok(None);
    }
    Ok(SegmentHeader::unpack(payload).ok())
}

/// A refcounted reference to one open segment file, unlinked at the last drop once doomed
#[derive(Clone)]
pub struct SegmentHandle {
    inner: Arc<SegmentInner>,
}

impl SegmentHandle {
    /// A handle to an open segment file whose records lie in this layout
    pub fn new(
        id: SegmentId,
        path: PathBuf,
        file: FileId,
        driver: Arc<IoDriver>,
        layout: RecordLayout,
    ) -> SegmentHandle {
        SegmentHandle {
            inner: Arc::new(SegmentInner {
                id,
                path,
                file,
                driver,
                layout,
                is_doomed: AtomicBool::new(false),
                mapping: OnceLock::new(),
            }),
        }
    }

    /// A handle on a segment file, its layout read off the header record it opens with
    pub fn opened(
        id: SegmentId,
        path: PathBuf,
        file: FileId,
        driver: Arc<IoDriver>,
    ) -> Result<SegmentHandle> {
        // A file with no readable header reads as keyed, so its keyless records fail their checks
        let layout =
            read_segment_header(&driver, file)?.map_or(RecordLayout::Keyed, |header| header.layout);
        Ok(SegmentHandle::new(id, path, file, driver, layout))
    }

    /// How this segment frames its records
    pub fn layout(&self) -> RecordLayout {
        self.inner.layout
    }

    /// A never-doomed stand-in a tail holds until it opens its first real segment
    pub fn placeholder(driver: Arc<IoDriver>) -> SegmentHandle {
        SegmentHandle::new(
            SegmentId(0),
            PathBuf::new(),
            NO_FILE,
            driver,
            RecordLayout::Keyed,
        )
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

    /// The segment's read-only mapping of `span` bytes, made on the first ask and then kept
    pub fn mapping(&self, span: u64) -> Option<&Mapping> {
        self.inner
            .mapping
            .get_or_init(|| Mapping::open(&self.inner.path, span).map(Arc::new))
            .as_deref()
    }

    /// The same mapping by count, for a reader that keeps it after letting the handle go
    pub fn shared_mapping(&self, span: u64) -> Option<Arc<Mapping>> {
        self.mapping(span)?;
        self.inner.mapping.get().and_then(|held| held.clone())
    }
}

/// A bounded cache of open sealed segment handles, reclaimed by second chance
pub struct FdCache {
    id: u64,
    entries: Hold<SegmentHandle>,
}

/// The next cache's id, so two volumes never share a memo
static NEXT_CACHE_ID: AtomicU64 = AtomicU64::new(0);

/// A handle a thread resolved, held weakly so the memo never keeps a file alive
struct HandleMemo {
    cache: u64,
    segment: SegmentId,
    handle: std::sync::Weak<SegmentInner>,
}

/// Each thread remembers this many handles by segment id, so repeat reads skip the map's guard
const MEMO_SLOTS: usize = 64;

thread_local! {
    static HANDLES: RefCell<[Option<HandleMemo>; MEMO_SLOTS]> =
        const { RefCell::new([const { None }; MEMO_SLOTS]) };
}

/// A cheap hash for segment ids, which this volume allocates in order
#[derive(Clone, Copy, Default)]
pub struct SegmentIdHash;

impl std::hash::BuildHasher for SegmentIdHash {
    type Hasher = SegmentIdHasher;

    fn build_hasher(&self) -> SegmentIdHasher {
        SegmentIdHasher(0)
    }
}

/// The `SegmentIdHash` hasher, holding the mix as it goes
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
            id: NEXT_CACHE_ID.fetch_add(1, Ordering::Relaxed),
            entries: Hold::new(capacity.max(1), 1),
        }
    }

    /// Fetch a cached handle, giving it another chance at the next sweep
    pub fn get(&self, id: SegmentId) -> Option<SegmentHandle> {
        let at = id.0 as usize % MEMO_SLOTS;
        let memoized = HANDLES.with(|slots| {
            slots.borrow()[at]
                .as_ref()
                .filter(|memo| memo.cache == self.id && memo.segment == id)
                .and_then(|memo| memo.handle.upgrade())
                .map(|inner| SegmentHandle { inner })
        });
        if memoized.is_some() {
            return memoized;
        }

        let handle = self.entries.get(segment_key(id))?;
        HANDLES.with(|slots| {
            slots.borrow_mut()[at] = Some(HandleMemo {
                cache: self.id,
                segment: id,
                handle: Arc::downgrade(&handle.inner),
            });
        });
        Some(handle)
    }

    /// Insert a handle, giving up a cold one when at capacity
    pub fn insert(&self, handle: SegmentHandle) {
        let key = segment_key(handle.id());
        self.entries.insert(key, handle, 1);
    }

    /// Take a segment's handle out of the cache, as when the segment is doomed
    pub fn remove(&self, id: SegmentId) -> Option<SegmentHandle> {
        self.entries.take(segment_key(id))
    }

    /// Drop every cached handle, for a reader rebuilding its view of the volume
    pub fn clear(&self) {
        self.entries.clear();
    }

    /// Number of handles currently cached
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the cache holds no handles
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
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

    /// A waker that counts how often it is woken
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

    /// Open a file holding these bytes
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
                },
                Op::Open {
                    tag: Tag(11),
                    path: dir.join("b"),
                    create: true,
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

        let handle = SegmentHandle::new(
            SegmentId(1),
            path.clone(),
            file,
            Arc::clone(&driver),
            RecordLayout::Keyed,
        );
        let reader = handle.clone();
        handle.mark_doomed();

        drop(handle);
        assert_eq!(list(&driver, dir).len(), 1);

        drop(reader);
        assert!(list(&driver, dir).is_empty());
    }

    // the memo answers a repeat, pins nothing, and never answers for another cache
    #[test]
    fn a_repeat_resolution_is_memoized_without_pinning() {
        let driver = driver();
        let dir = Path::new("/reel");
        let path = dir.join("000009.reel");
        let file = open(&driver, &path);
        let handle = SegmentHandle::new(
            SegmentId(9),
            path,
            file,
            Arc::clone(&driver),
            RecordLayout::Keyed,
        );

        let cache = FdCache::new(4);
        cache.insert(handle.clone());

        let first = cache.get(SegmentId(9)).expect("first get");
        let repeat = cache.get(SegmentId(9)).expect("memoized get");
        assert_eq!(first.id(), repeat.id());

        // After removal the weak memo has nothing to upgrade once the readers drain
        cache.remove(SegmentId(9));
        drop(handle);
        drop(first);
        drop(repeat);
        assert!(
            cache.get(SegmentId(9)).is_none(),
            "nothing left to upgrade once readers drain"
        );

        // A second cache on the same thread is never answered by the first one's memo
        let other = FdCache::new(4);
        assert!(other.get(SegmentId(9)).is_none());
    }

    // reads moving between segments keep each one memoized
    #[test]
    fn reads_across_segments_each_stay_memoized() {
        let driver = driver();
        let dir = Path::new("/reel");
        let cache = FdCache::new(4);
        let handles: Vec<SegmentHandle> = (1..=2u32)
            .map(|number| {
                let path = dir.join(format!("{number:06}.reel"));
                let file = open(&driver, &path);
                let handle = SegmentHandle::new(
                    SegmentId(number),
                    path,
                    file,
                    Arc::clone(&driver),
                    RecordLayout::Keyed,
                );
                cache.insert(handle.clone());
                handle
            })
            .collect();
        assert!(cache.get(SegmentId(1)).is_some());
        assert!(cache.get(SegmentId(2)).is_some());

        // Out of the map, each one still answers from the memo while its handle lives
        cache.remove(SegmentId(1));
        cache.remove(SegmentId(2));
        assert_eq!(cache.get(SegmentId(1)).map(|h| h.id()), Some(SegmentId(1)));
        assert_eq!(cache.get(SegmentId(2)).map(|h| h.id()), Some(SegmentId(2)));

        drop(handles);
        assert!(cache.get(SegmentId(1)).is_none());
        assert!(cache.get(SegmentId(2)).is_none());
    }

    // a live segment that is never doomed survives every handle drop
    #[test]
    fn live_segment_survives() {
        let driver = driver();
        let dir = Path::new("/reel");
        let path = dir.join("000002.reel");
        let file = open(&driver, &path);

        let handle = SegmentHandle::new(
            SegmentId(2),
            path,
            file,
            Arc::clone(&driver),
            RecordLayout::Keyed,
        );

        drop(handle);
        assert_eq!(list(&driver, dir).len(), 1);
    }

    // at capacity the cache evicts the handle no read has touched since its insert
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
                RecordLayout::Keyed,
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
            RecordLayout::Keyed,
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
            RecordLayout::Keyed,
        ));
        assert!(cache.remove(SegmentId(9)).is_some());

        assert!(cache.is_empty());
        assert!(cache.get(SegmentId(9)).is_none());
    }

    // a batch wider than the table goes down in runs
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

        // Refusing the self-drain forces the out-of-band path, so another thread drains
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

        // Refused, so both futures stay pending until one reap drains both completions
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

        // Refused, so the polls stay pending and the waker count can be checked
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
        // Refused, so the futures stay pending on the passive path
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
        // Refused, so the futures stay pending on the passive path
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
        // Refused, so the futures stay pending on the passive path
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

            // Two reads are in and the delayed one is still out, so the drop puts back both
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

    // a wrapped claim yields to the caller without parking it
    #[test]
    fn a_wrapped_claim_yields_rather_than_parking() {
        // Refused, so the futures stay pending on the passive path
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

        // The holder's completion stays in its slot, and burnt tags put the next op a table on
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

        // Only this thread makes the take the claim is waiting for
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
