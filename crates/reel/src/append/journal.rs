//! An open segment's journal rows, kept in the segment's own file past every record and footer

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::error::Result;
use crate::format::journal::{push_group, JournalRow};
use crate::format::record::BLOCK;
use crate::io::op::{FileId, WriteBuf};
use crate::io::ServingBackend;
use crate::reel::segment::IoDriver;
use crate::sync::{lock, try_lock};

/// The rows region is zeroed at most this far at a time ahead of its rows, and a sixteenth of its segment when less
const FILL: u64 = 1024 * 1024;

/// How a volume puts journal rows down
#[derive(Clone, Copy, Debug, Default)]
pub(super) struct Writes {
    /// Each push writes its group before it returns
    pub through: bool,

    /// Every write covers whole blocks
    pub whole_blocks: bool,
}

/// One open segment's journal, a region of the segment file from `rows_at` on
pub(super) struct Journal {
    /// The driver the file is written through
    driver: Arc<IoDriver>,

    /// The segment file the rows go into, none for a tail that holds no segment yet
    file: Option<FileId>,

    /// Where the rows region begins in the segment file
    rows_at: u64,

    /// Groups for records that have landed and are not in the file yet
    pending: Mutex<Vec<u8>>,

    /// How far into the region the rows are written, held across a write so groups land in order
    written: Mutex<u64>,

    /// Bytes pushed so far, written or pending, which the segment's room shrinks by
    pushed: AtomicU64,

    /// Whether each push writes its group before it returns, so a process crash keeps every row its record kept
    through: bool,

    /// Whether a write covers whole blocks, its end padded with zeros
    whole_blocks: bool,

    /// How far into the region the file is zeroed
    filled: AtomicU64,

    /// The segment's size, which the zeroed region never runs past
    span: u64,
}

impl Journal {
    /// The journal of a segment being drawn, writing each group as it comes when `through`
    pub(super) fn create(
        driver: &Arc<IoDriver>,
        file: FileId,
        rows_at: u64,
        span: u64,
        writes: Writes,
    ) -> Journal {
        Journal::over(driver, Some(file), rows_at, 0, span, writes)
    }

    /// Take a journal up again past the whole groups a reopen read
    ///
    /// A whole-block volume zeroes the rest of the block the last group ends in, so the
    /// next write opens on the boundary and the read steps over the zeros to it.
    pub(super) fn resume(
        driver: &Arc<IoDriver>,
        file: FileId,
        rows_at: u64,
        written: u64,
        span: u64,
        writes: Writes,
    ) -> Result<Journal> {
        let mut journal = Journal::over(driver, Some(file), rows_at, written, span, writes);
        if journal.whole_blocks && !written.is_multiple_of(BLOCK) {
            let start = written - written % BLOCK;
            let mut block = driver.pread(file, rows_at + start, written - start)?;
            block.resize(BLOCK as usize, 0);
            driver.writev_all(file, rows_at + start, vec![WriteBuf::owned(block)])?;
            let next = start + BLOCK;
            *journal
                .written
                .get_mut()
                .unwrap_or_else(|held| held.into_inner()) = next;
            journal.pushed.store(next, Ordering::Release);
            journal.filled.store(next, Ordering::Release);
        }
        Ok(journal)
    }

    /// A journal with no file, for a tail that holds no segment yet
    pub(super) fn none(driver: &Arc<IoDriver>) -> Journal {
        Journal::over(driver, None, 0, 0, 0, Writes::default())
    }

    fn over(
        driver: &Arc<IoDriver>,
        file: Option<FileId>,
        rows_at: u64,
        written: u64,
        span: u64,
        writes: Writes,
    ) -> Journal {
        Journal {
            driver: Arc::clone(driver),
            file,
            rows_at,
            pending: Mutex::new(Vec::new()),
            written: Mutex::new(written),
            pushed: AtomicU64::new(written),
            through: writes.through,
            whole_blocks: writes.whole_blocks,
            filled: AtomicU64::new(written),
            span,
        }
    }

    /// Where the rows region begins, which the seal's footer must stay below
    pub(super) fn rows_at(&self) -> u64 {
        self.rows_at
    }

    /// Bytes the journal holds once everything pushed is written
    pub(super) fn len(&self) -> u64 {
        self.pushed.load(Ordering::Acquire)
    }

    /// Add the rows of one write that has landed, as one group
    pub(super) fn push(&self, rows: &[JournalRow]) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let mut pending = lock(&self.pending);
        let (Some(file), true) = (self.file, self.through) else {
            let before = pending.len();
            push_group(rows, &mut pending);
            self.pushed
                .fetch_add((pending.len() - before) as u64, Ordering::AcqRel);
            return Ok(());
        };
        // Groups go into the file in push order under the lock, so a crash cuts only the last
        let mut group = Vec::new();
        push_group(rows, &mut group);
        let at = self.pushed.load(Ordering::Acquire);
        let end = at + group.len() as u64;
        self.fill_to(file, end)?;
        self.driver
            .writev_all(file, self.rows_at + at, vec![WriteBuf::owned(group)])?;
        self.pushed.store(end, Ordering::Release);
        Ok(())
    }

    /// Write every pending group into the segment file, whose own sync then covers them
    pub(super) fn write_pending(&self) -> Result<()> {
        let mut written = lock(&self.written);
        self.write_locked(&mut written)
    }

    /// The same write, skipped while another caller holds the region
    pub(super) fn try_write_pending(&self) {
        if let Some(mut written) = try_lock(&self.written) {
            let _ = self.write_locked(&mut written);
        }
    }

    fn write_locked(&self, written: &mut u64) -> Result<()> {
        let Some(file) = self.file else {
            return Ok(());
        };
        let mut bytes = std::mem::take(&mut *lock(&self.pending));
        if bytes.is_empty() {
            return Ok(());
        }
        if self.whole_blocks {
            let padded = (bytes.len() as u64).next_multiple_of(BLOCK);
            self.pushed
                .fetch_add(padded - bytes.len() as u64, Ordering::AcqRel);
            bytes.resize(padded as usize, 0);
        }
        let len = bytes.len() as u64;
        self.fill_to(file, *written + len)?;
        self.driver
            .writev_all(file, self.rows_at + *written, vec![WriteBuf::owned(bytes)])?;
        *written += len;
        Ok(())
    }

    /// Zero the region out past `end` before rows land there, so a sync of them settles no extents
    fn fill_to(&self, file: FileId, end: u64) -> Result<()> {
        let filled = self.filled.load(Ordering::Acquire);
        if end <= filled || matches!(self.driver.serving(), ServingBackend::Sim) {
            return Ok(());
        }
        let step = (self.span / 16).clamp(BLOCK, FILL).next_multiple_of(BLOCK);
        let to = (end.div_ceil(step) * step).min(self.span.max(end).next_multiple_of(BLOCK));
        self.driver.writev_all(
            file,
            self.rows_at + filled,
            vec![WriteBuf::zeros((to - filled) as usize)],
        )?;
        self.driver.sync_full(file)?;
        self.filled.store(to, Ordering::Release);
        Ok(())
    }

    /// Drop what is pending once the segment's footer lists everything the rows did
    pub(super) fn remove(&self) {
        lock(&self.pending).clear();
    }
}

impl Drop for Journal {
    /// Write what is pending on the way out, so a store dropped without a seal leaves its rows
    fn drop(&mut self) {
        let _ = self.write_pending();
    }
}
