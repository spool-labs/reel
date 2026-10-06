//! An open segment's journal file, mapped where the tail is so rows land with their records

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::error::Result;
use crate::format::journal::{journal_path, push_group, JournalRow};
use crate::io::mapping::WriteMapping;
use crate::io::op::{FileId, WriteBuf};
use crate::reel::segment::IoDriver;
use crate::sync::{lock, try_lock};

/// A mapped journal's file grows by this much at a time, so a reopen reads little past its rows
const GROW: u64 = 1024 * 1024;

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

    /// The file mapped writable, so a process crash keeps every row its record kept
    mapped: Option<MappedRows>,
}

struct JournalFile {
    id: Option<FileId>,
    written: u64,
}

struct MappedRows {
    /// The writable mapping rows are copied into
    map: WriteMapping,

    /// Bytes the mapping spans, past which a group goes down through the driver
    span: u64,

    /// The file under the mapping, open until the seal
    id: FileId,

    /// The file's length, kept one step ahead of the rows copied in
    grown: AtomicU64,
}

impl Journal {
    /// Create the journal of a segment being drawn, mapped over `span` bytes when the tail is mapped
    pub(super) fn create(
        driver: &Arc<IoDriver>,
        segment_path: &Path,
        span: Option<u64>,
    ) -> Result<Journal> {
        let path = journal_path(segment_path);
        // A drawn number is new and an open unlinks stale journals, so the file starts empty
        let id = driver.open(&path, true)?;
        let mapped = span
            .and_then(|span| WriteMapping::growing(&path, span).map(|map| (map, span)))
            .map(|(map, span)| MappedRows {
                map,
                span,
                id,
                grown: AtomicU64::new(0),
            });
        Ok(Journal::over(driver, path, Some(id), 0, mapped))
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
        mapped: Option<MappedRows>,
    ) -> Journal {
        Journal {
            driver: Arc::clone(driver),
            path,
            pending: Mutex::new(Vec::new()),
            file: Mutex::new(JournalFile { id, written }),
            pushed: AtomicU64::new(written),
            mapped,
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
        let Some(mapped) = &self.mapped else {
            let before = pending.len();
            push_group(rows, &mut pending);
            self.pushed
                .fetch_add((pending.len() - before) as u64, Ordering::AcqRel);
            return Ok(());
        };
        // Groups go into the file in push order under the lock, so a crash cuts only the last
        pending.clear();
        push_group(rows, &mut pending);
        let at = self.pushed.load(Ordering::Acquire);
        let end = at + pending.len() as u64;
        if end > mapped.grown.load(Ordering::Acquire) {
            let grown = (end.div_ceil(GROW) * GROW).min(mapped.span.max(end));
            self.driver.truncate(mapped.id, grown)?;
            mapped.grown.store(grown, Ordering::Release);
        }
        if !mapped.map.write(at, &pending) {
            let group = std::mem::take(&mut *pending);
            self.driver
                .writev_all(mapped.id, at, vec![WriteBuf::owned(group)])?;
        }
        pending.clear();
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
            // A mapped file runs a step past its rows, so a clean close trims it
            if self.mapped.is_some() {
                let _ = self
                    .driver
                    .truncate(id, self.pushed.load(Ordering::Acquire));
            }
            let _ = self.driver.close(id);
        }
    }
}
