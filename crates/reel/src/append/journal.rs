//! An open segment's journal file, written through where a dead process should leave its rows

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::error::Result;
use crate::format::journal::{journal_path, push_group, JournalRow};
use crate::io::op::{FileId, WriteBuf};
use crate::reel::segment::IoDriver;
use crate::sync::{lock, try_lock};

/// A written-through journal is zeroed at most this far at a time ahead of its rows, and a sixteenth of its segment when less
const FILL: u64 = 1024 * 1024;

/// One open segment's journal
pub(super) struct Journal {
    /// The driver the file is written through
    driver: Arc<IoDriver>,

    /// Where the journal sits, beside its segment
    path: PathBuf,

    /// Groups for records that have landed and are not in the file yet
    pending: Mutex<Vec<u8>>,

    /// The file and how far it is written, held across a write so groups land in order
    file: Mutex<JournalFile>,

    /// Bytes pushed so far, written or pending, which the segment's room shrinks by
    pushed: AtomicU64,

    /// The file each push writes its group into before it returns, so a process crash keeps every row its record kept
    through: Option<FileId>,

    /// How far the written-through file is zeroed
    filled: AtomicU64,

    /// The segment's size, which the zeroed file never runs past
    span: u64,
}

struct JournalFile {
    id: Option<FileId>,
    written: u64,
}

impl Journal {
    /// Create the journal of a segment of `span` bytes being drawn, writing each group as it comes when `through`
    pub(super) fn create(
        driver: &Arc<IoDriver>,
        segment_path: &Path,
        span: u64,
        through: bool,
    ) -> Result<Journal> {
        let path = journal_path(segment_path);
        // A drawn number is new and an open unlinks stale journals, so the file starts empty
        let id = driver.open(&path, true)?;
        let mut journal = Journal::over(driver, path, Some(id), 0, through.then_some(id));
        journal.span = span;
        Ok(journal)
    }

    /// Take a journal up again with only the rows a reopen accepted
    pub(super) fn resume(
        driver: &Arc<IoDriver>,
        segment_path: &Path,
        rows: &[JournalRow],
    ) -> Result<Journal> {
        let path = journal_path(segment_path);
        let fresh = path.with_extension("rows.part");
        let id = driver.open(&fresh, true)?;
        driver.truncate(id, 0)?;
        let mut bytes = Vec::new();
        if !rows.is_empty() {
            push_group(rows, &mut bytes);
        }
        let written = bytes.len() as u64;
        if written > 0 {
            driver.writev_all(id, 0, vec![WriteBuf::owned(bytes)])?;
        }
        driver.sync_data(id)?;
        // The rename swaps whole journals, so a crash leaves one or the other
        driver.rename(&fresh, &path)?;
        if let Some(dir) = path.parent() {
            driver.sync_dir(dir)?;
        }
        Ok(Journal::over(driver, path, Some(id), written, None))
    }

    /// A journal with no file, for a tail that holds no segment yet
    pub(super) fn none(driver: &Arc<IoDriver>) -> Journal {
        Journal::over(driver, PathBuf::new(), None, 0, None)
    }

    fn over(
        driver: &Arc<IoDriver>,
        path: PathBuf,
        id: Option<FileId>,
        written: u64,
        through: Option<FileId>,
    ) -> Journal {
        Journal {
            driver: Arc::clone(driver),
            path,
            pending: Mutex::new(Vec::new()),
            file: Mutex::new(JournalFile { id, written }),
            pushed: AtomicU64::new(written),
            through,
            filled: AtomicU64::new(0),
            span: 0,
        }
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
        let Some(id) = self.through else {
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
        let filled = self.filled.load(Ordering::Acquire);
        if end > filled {
            // A sync over blocks the file already holds settles no extents, as the segment's fill does for its records
            let step = (self.span / 16).clamp(4096, FILL);
            let to = (end.div_ceil(step) * step).min(self.span.max(end));
            self.driver
                .writev_all(id, filled, vec![WriteBuf::zeros((to - filled) as usize)])?;
            self.driver.sync_full(id)?;
            self.filled.store(to, Ordering::Release);
        }
        self.driver
            .writev_all(id, at, vec![WriteBuf::owned(group)])?;
        self.pushed.store(end, Ordering::Release);
        Ok(())
    }

    /// Write every pending group and sync, under the file's lock so a seal waits for it
    pub(super) fn sync_pending(&self) -> Result<()> {
        let mut file = lock(&self.file);
        self.write_locked(&mut file)?;
        match file.id {
            Some(id) => self.driver.sync_data(id),
            None => Ok(()),
        }
    }

    /// The same write, skipped while another caller holds the file
    pub(super) fn try_write_pending(&self) {
        if let Some(mut file) = try_lock(&self.file) {
            let _ = self.write_locked(&mut file);
        }
    }

    fn write_locked(&self, file: &mut JournalFile) -> Result<()> {
        let Some(id) = file.id else {
            return Ok(());
        };
        let bytes = std::mem::take(&mut *lock(&self.pending));
        if bytes.is_empty() {
            return Ok(());
        }
        let len = bytes.len() as u64;
        self.driver
            .writev_all(id, file.written, vec![WriteBuf::owned(bytes)])?;
        file.written += len;
        Ok(())
    }

    /// Close and unlink the journal, once the segment's footer lists everything it held
    pub(super) fn remove(&self) {
        let mut file = lock(&self.file);
        lock(&self.pending).clear();
        if let Some(id) = file.id.take() {
            let _ = self.driver.close(id);
            let _ = self.driver.unlink(&self.path);
        }
    }
}

impl Drop for Journal {
    /// Write what is pending on the way out, so a store dropped without a seal leaves its rows
    fn drop(&mut self) {
        let mut file = lock(&self.file);
        let _ = self.write_locked(&mut file);
        if let Some(id) = file.id.take() {
            let _ = self.driver.close(id);
        }
    }
}
