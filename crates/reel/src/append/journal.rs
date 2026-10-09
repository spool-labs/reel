//! Journal rows for an open segment, stored in the segment file after the records

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::error::Result;
use crate::format::journal::{push_group, JournalRow};
use crate::format::record::BLOCK;
use crate::io::op::{FileId, WriteBuf};
use crate::io::ServingBackend;
use crate::reel::segment::IoDriver;
use crate::sync::{lock, try_lock};

/// Zero the rows region in steps of a sixteenth of the segment, at most this many bytes
const FILL: u64 = 1024 * 1024;

/// The journal of one open segment, stored from `rows_at` to the end of the segment file
pub(super) struct Journal {
    /// The driver the file is written through
    driver: Arc<IoDriver>,

    /// The segment file, or none before the tail has a segment
    file: Option<FileId>,

    /// Offset in the segment file where the rows start
    rows_at: u64,

    /// Groups for records that have landed and are not in the file yet
    pending: Mutex<Vec<u8>>,

    /// Bytes of rows written so far, locked during a write so groups stay in order
    written: Mutex<u64>,

    /// Bytes pushed so far, written or pending, which the segment's room shrinks by
    pushed: AtomicU64,

    /// Whether each write is padded with zeros to a whole block
    whole_blocks: bool,

    /// Bytes of the rows region already zeroed
    filled: AtomicU64,

    /// The segment size, which also caps how far the rows region gets zeroed
    span: u64,
}

impl Journal {
    /// Start the journal for a new segment
    pub(super) fn create(
        driver: &Arc<IoDriver>,
        file: FileId,
        rows_at: u64,
        span: u64,
        whole_blocks: bool,
    ) -> Journal {
        Journal::over(driver, Some(file), rows_at, 0, span, whole_blocks)
    }

    /// Continue a reopened segment's journal after its last whole group
    pub(super) fn resume(
        driver: &Arc<IoDriver>,
        file: FileId,
        rows_at: u64,
        written: u64,
        span: u64,
        whole_blocks: bool,
    ) -> Result<Journal> {
        let mut journal = Journal::over(driver, Some(file), rows_at, written, span, whole_blocks);
        // Zero the rest of the last block, so the next group starts on a block boundary
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
        Journal::over(driver, None, 0, 0, 0, false)
    }

    fn over(
        driver: &Arc<IoDriver>,
        file: Option<FileId>,
        rows_at: u64,
        written: u64,
        span: u64,
        whole_blocks: bool,
    ) -> Journal {
        Journal {
            driver: Arc::clone(driver),
            file,
            rows_at,
            pending: Mutex::new(Vec::new()),
            written: Mutex::new(written),
            pushed: AtomicU64::new(written),
            whole_blocks,
            filled: AtomicU64::new(written),
            span,
        }
    }

    /// Offset where the rows start, which the footer must end before
    pub(super) fn rows_at(&self) -> u64 {
        self.rows_at
    }

    /// Bytes the journal holds once everything pushed is written
    pub(super) fn len(&self) -> u64 {
        self.pushed.load(Ordering::Acquire)
    }

    /// Add the rows of one landed write as one group, for the next pace or flush to write
    pub(super) fn push(&self, rows: &[JournalRow]) {
        if rows.is_empty() {
            return;
        }
        let mut pending = lock(&self.pending);
        let before = pending.len();
        push_group(rows, &mut pending);
        self.pushed
            .fetch_add((pending.len() - before) as u64, Ordering::AcqRel);
    }

    /// Write all pending groups to the segment file for the next sync to cover
    pub(super) fn write_pending(&self) -> Result<()> {
        let mut written = lock(&self.written);
        self.write_locked(&mut written)
    }

    /// Same as write_pending, but skip it if another caller is writing
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

    /// Zero the rows region up to `end` first, so syncing rows needs no block allocation
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
        // Some filesystems fill a gap up to the next write, so free the space past the segment size
        if filled == 0 {
            let past = self.span.next_multiple_of(BLOCK);
            self.driver
                .release(file, past, self.rows_at.saturating_sub(past))?;
        }
        self.filled.store(to, Ordering::Release);
        Ok(())
    }

    /// Write what is pending, then a seal's mark in the block after the last row
    pub(super) fn mark_sealed(&self, mark: &[u8]) -> Result<()> {
        let Some(file) = self.file else {
            return Ok(());
        };
        let mut written = lock(&self.written);
        self.write_locked(&mut written)?;
        let at = (self.rows_at + *written).next_multiple_of(BLOCK);
        let mut block = vec![0u8; BLOCK as usize];
        block[BLOCK as usize - mark.len()..].copy_from_slice(mark);
        self.driver
            .writev_all(file, at, vec![WriteBuf::owned(block)])
    }

    /// Give back the zeros laid ahead of the written rows, at a close that writes nothing more
    pub(super) fn release_ahead(&self) -> Result<()> {
        let Some(file) = self.file else {
            return Ok(());
        };
        let written = lock(&self.written).next_multiple_of(BLOCK);
        let filled = self.filled.load(Ordering::Acquire);
        self.driver
            .release(file, self.rows_at + written, filled.saturating_sub(written))
    }

    /// Drop pending rows once the sealed footer lists them all
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
